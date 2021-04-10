package main

import (
	"bytes"
	"context"
	"crawshaw.io/sqlite"
	"crawshaw.io/sqlite/sqlitex"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/davecgh/go-spew/spew"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestMoo(t *testing.T) {
	ctx := context.Background()

	dbpool, err := sqlitex.Open("file:memory:?mode=memory", 0, 10)
	require.NoError(t, err)

	conn := dbpool.Get(ctx)
	defer dbpool.Put(conn)

	conn.CreateFunction("array_concat", true, 2, func(c sqlite.Context, values ...sqlite.Value) {
		jsonText := values[0].Text()
		delimiter := values[1].Text()

		slice := []string{}
		err := json.Unmarshal([]byte(jsonText), &slice)
		if err != nil {
			c.ResultError(err)
		}

		joined := strings.Join(slice, delimiter)
		c.ResultText(joined)
	}, nil, nil)

	err = sqlitex.Exec(conn, `create virtual table mytable using fts5(pk, sk unindexed, body)`, nil)
	//err = sqlitex.Exec(conn, `create table mytable(id integer primary key, body text)`, nil)
	//err = sqlitex.Exec(conn, `create table mytable(pk text, sk text, body text)`, nil)
	require.NoError(t, err)
	//require.NoError(t, err)

	sess, err := conn.CreateSession("")
	require.NoError(t, err)
	defer sess.Delete()

	err = sess.Attach("")
	require.NoError(t, err)

	err = sqlitex.Exec(conn, `insert into mytable(pk, sk, body) values('mypk', 'mysk', 'hello world')`, nil)
	//err = sqlitex.Exec(conn, `insert into mytable(pk, sk, body) values('mypk', 'mysk', '{"attrA": {"S": "string value"}, "attrB": {"SS": ["first", "second"]}}')`, nil)
	require.NoError(t, err)

	err = sqlitex.Exec(conn, `insert into mytable(pk, sk, body) values('mypk', 'mysk2', 'second hello')`, nil)
	require.NoError(t, err)

	err = sqlitex.Exec(conn, `update mytable set body = 'third hello' where pk = 'mypk' and sk = 'mysk2'`, nil)
	require.NoError(t, err)

//	err = sqlitex.Exec(conn, `
//with mycte as (
//select
//
//)
//insert into mytable(pk, sk, body) values('mypk', 'mysk', '{"attrA": {"S": "string value"}, "attrB": {"SS": ["first", "second"]}}')
//`, nil)

	//stmt, _, err := conn.PrepareTransient(`
	//	select
	//		json_extract(body, '$.attrB.SS') as mycol,
	//		array_concat(json_extract(body, '$.attrB.SS'), ' ') as second
	//	from mytable
	//`)
	stmt, _, err := conn.PrepareTransient(`select * from mytable('mypk')`)
	require.NoError(t, err)
	defer stmt.Finalize()

	results := []map[string]string{}
	for {
		hasRow, err := stmt.Step()
		require.NoError(t, err)

		if !hasRow {
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

	spew.Config.SortKeys = true
	spew.Dump(results)

	buf := &bytes.Buffer{}
	err = sess.Changeset(buf)
	require.NoError(t, err)

	fmt.Println(base64.StdEncoding.EncodeToString(buf.Bytes()))
}
