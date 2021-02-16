package changesets

import (
	"bytes"
	"context"
	"crawshaw.io/sqlite"
	"github.com/pkg/errors"
	"io"
)

type Reader interface {
	GetChangeset(ctx context.Context) (io.Reader, error)
}

type Writer interface {
	PutChangeset(ctx context.Context, r io.Reader) error
}

type ReadWriter interface {
	Writer
	Reader
}

func appendChangeset(accumulator, scratch *bytes.Buffer, next []byte) error {
	err := sqlite.ChangesetConcat(scratch, accumulator, bytes.NewReader(next))
	if err != nil {
		return errors.WithStack(err)
	}

	accumulator.Reset()
	_, err = io.Copy(accumulator, scratch)
	if err != nil {
		return errors.WithStack(err)
	}

	scratch.Reset()
	return nil
}
