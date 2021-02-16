package changesets

import (
	"bytes"
	"context"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	"io"
	"io/ioutil"
	"math/rand"
	"time"
)

type dynamoChangesets struct {
	api      dynamodbiface.DynamoDBAPI
	table    string
	startKey *string
	entropy  io.Reader
}

func NewDynamoChangesets(api dynamodbiface.DynamoDBAPI, table string) *dynamoChangesets {
	t := time.Now()
	entropy := ulid.Monotonic(rand.New(rand.NewSource(t.UnixNano())), 0)
	return &dynamoChangesets{api: api, table: table, entropy: entropy}
}

func (d *dynamoChangesets) PutChangeset(ctx context.Context, r io.Reader) error {
	body, err := ioutil.ReadAll(r)
	if err != nil {
		return errors.WithStack(err)
	}

	pk := "changesets"
	sk := ulid.MustNew(ulid.Timestamp(time.Now()), d.entropy).String()

	_, err = d.api.PutItemWithContext(ctx, &dynamodb.PutItemInput{
		TableName: &d.table,
		Item: map[string]*dynamodb.AttributeValue{
			"pk": {S: &pk},
			"sk": {S: &sk},
			"cs": {B: body},
		},
	})
	return errors.WithStack(err)
}

func (d *dynamoChangesets) GetChangeset(ctx context.Context) (io.Reader, error) {
	var esk map[string]*dynamodb.AttributeValue
	if d.startKey != nil {
		esk = map[string]*dynamodb.AttributeValue{
			"pk": {S: aws.String("changesets")},
			"sk": {S: d.startKey},
		}
	}

	input := &dynamodb.QueryInput{
		//ConsistentRead:    nil,
		TableName:              &d.table,
		KeyConditionExpression: aws.String("pk = :pk"),
		ExclusiveStartKey:      esk,
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":pk": {S: aws.String("changesets")},
		},
	}

	changeset := &bytes.Buffer{}
	buf := &bytes.Buffer{}

	err := d.api.QueryPagesWithContext(ctx, input, func(page *dynamodb.QueryOutput, lastPage bool) bool {
		for _, item := range page.Items {
			err := appendChangeset(changeset, buf, item["cs"].B)
			if err != nil {
				panic(err)
			}
			d.startKey = item["sk"].S
		}

		return !lastPage
	})
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if changeset.Len() > 0 {
		return changeset, nil
	} else {
		return nil, nil
	}
}
