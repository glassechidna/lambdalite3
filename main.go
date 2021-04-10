package main

import (
	"bytes"
	"context"
	"crawshaw.io/sqlite"
	"crawshaw.io/sqlite/sqlitex"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/glassechidna/lambdalite3/changesets"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"
)

func main() {
	sess, err := session.NewSessionWithOptions(session.Options{
		Profile:                 os.Getenv("AWS_PROFILE"),
		SharedConfigState:       session.SharedConfigEnable,
		AssumeRoleTokenProvider: stscreds.StdinTokenProvider,
	})
	if err != nil {
		panic(err)
	}

	bucket := os.Getenv("BUCKET")
	key := os.Getenv("KEY")
	versionId := os.Getenv("VERSION_ID")
	table := os.Getenv("TABLE")

	var cs changesets.ReadWriter
	cs = changesets.NewDynamoChangesets(dynamodb.New(sess), table)
	//cs = &staleChangesets{Reader: cs, lastRefreshed: time.Now(), window: time.Second * 6}
	l := NewLambdalite3(cs)

	err = l.downloadDatabase(context.Background(), s3.New(sess), bucket, key, versionId)
	if err != nil {
		panic(err)
	}

	lambda.Start(l.handleQuery)
}

type Lambdalite3 struct {
	dbpool       *sqlitex.Pool
	cs           changesets.ReadWriter
	downloadTime float64
	warm         bool
}

func NewLambdalite3(cs changesets.ReadWriter) *Lambdalite3 {
	return &Lambdalite3{cs: cs}
}

//func (l *Lambdalite3) handleDynamoStream(ctx context.Context, event *events.DynamoDBEvent) error {
//	//for _, record := range event.Records {
//	//	/*
//	//
//	//	insert into dynamo_fts(pk, sk, attrA, attrB) values(
//	//
//	//	 */
//	//	//record.EventName
//	//	//record.Change.NewImage[""].String()
//	//}
//}

func (l *Lambdalite3) handleQuery(ctx context.Context, event *events.APIGatewayV2HTTPRequest) (*events.APIGatewayV2HTTPResponse, error) {
	query := event.QueryStringParameters["query"]
	if len(query) == 0 {
		return &events.APIGatewayV2HTTPResponse{
			StatusCode: 400,
			Body:       "must provide `query` query string parameter",
		}, nil
	}

	start := time.Now()

	conn := l.dbpool.Get(ctx)
	defer l.dbpool.Put(conn)

	changesCh := make(chan io.Reader, 1)
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		ch, err := l.cs.GetChangeset(gctx)
		changesCh <- ch
		return err
	})

	err := sqlitex.Exec(conn, "begin", nil)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	results, changesMade, err := l.execQuery(conn, query)
	if err != nil {
		return nil, err
	}

	err = g.Wait()
	if err != nil {
		return nil, err
	}

	changes := <-changesCh
	if changes != nil {
		fmt.Println("rerunning due to changesets available")

		err := sqlitex.Exec(conn, "rollback", nil)
		if err != nil {
			return nil, errors.WithStack(err)
		}

		err = conn.ChangesetApply(changes, nil, func(conflictType sqlite.ConflictType, iter sqlite.ChangesetIter) sqlite.ConflictAction {
			return sqlite.SQLITE_CHANGESET_REPLACE
		})
		if err != nil {
			return nil, errors.WithStack(err)
		}

		results, changesMade, err = l.execQuery(conn, query)
		if err != nil {
			return nil, err
		}
	} else {
		err := sqlitex.Exec(conn, "commit", nil)
		if err != nil {
			return nil, errors.WithStack(err)
		}
	}

	duration := float64(time.Now().Sub(start).Microseconds()) / 1000
	output := map[string]interface{}{
		"cold_start":       !l.warm,
		"results":          results,
		"query_time_ms":    duration,
		"download_time_ms": l.downloadTime,
	}
	if !l.warm {
		l.warm = true
	}

	if changesMade != nil {
		buf := changesMade.(*bytes.Buffer)
		output["changeset_bytes"] = buf.Len()
		err = l.cs.PutChangeset(ctx, changesMade)
		if err != nil {
			return nil, errors.WithStack(err)
		}
	}

	outputJson, _ := json.Marshal(output)
	return &events.APIGatewayV2HTTPResponse{
		StatusCode:      200,
		Headers:         map[string]string{"Content-type": "application/json"},
		Body:            string(outputJson),
		IsBase64Encoded: true,
	}, err
}

func (l *Lambdalite3) execQuery(conn *sqlite.Conn, query string) ([]map[string]string, io.Reader, error) {
	sess, err := conn.CreateSession("")
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	defer sess.Delete()

	err = sess.Attach("")
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}

	stmt, _, err := conn.PrepareTransient(query)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	defer stmt.Finalize()

	results := []map[string]string{}

	for {
		if hasRow, err := stmt.Step(); err != nil {
			return nil, nil, errors.WithStack(err)
		} else if !hasRow {
			break
		}

		row := map[string]string{}
		colLen := stmt.ColumnCount()
		for colIdx := 0; colIdx < colLen; colIdx++ {
			name := stmt.ColumnName(colIdx)
			val := stmt.GetText(name)
			row[name] = val
		}

		results = append(results, row)
	}

	buf := &bytes.Buffer{}
	err = sess.Changeset(buf)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}

	if buf.Len() == 0 {
		return results, nil, nil
	}

	return results, buf, nil
}

func (l *Lambdalite3) downloadDatabase(ctx context.Context, api s3iface.S3API, bucket string, key string, versionId string) error {
	dir, err := ioutil.TempDir("/tmp", "db*")
	if err != nil {
		return errors.WithStack(err)
	}

	path := filepath.Join(dir, "db.db")
	f, err := os.Create(path)
	if err != nil {
		return errors.WithStack(err)
	}

	start := time.Now()

	var versionIdPtr *string
	if len(versionId) > 0 {
		versionIdPtr = &versionId
	}

	downloader := s3manager.NewDownloaderWithClient(api)
	_, err = downloader.DownloadWithContext(ctx, f, &s3.GetObjectInput{
		Bucket:    &bucket,
		Key:       &key,
		VersionId: versionIdPtr,
	})
	if err != nil {
		return errors.WithStack(err)
	}

	l.downloadTime = float64(time.Now().Sub(start).Microseconds()) / 1000

	err = f.Close()
	if err != nil {
		return errors.WithStack(err)
	}

	l.dbpool, err = sqlitex.Open("file:"+path, 0, 10)
	if err != nil {
		return errors.WithStack(err)
	}

	return nil
}
