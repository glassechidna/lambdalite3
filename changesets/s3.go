package changesets

import (
	"bytes"
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/pkg/errors"
	"io"
)

type s3Changesets struct {
	api      s3iface.S3API
	bucket   string
	prefix   string
	startKey *string
}

func (s *s3Changesets) GetChangeset(ctx context.Context) (io.Reader, error) {
	downloader := s3manager.NewDownloaderWithClient(s.api)
	changeset := &bytes.Buffer{}
	buf := &bytes.Buffer{}

	err := s.api.ListObjectsV2PagesWithContext(ctx, &s3.ListObjectsV2Input{
		Bucket:     &s.bucket,
		Prefix:     &s.prefix,
		StartAfter: s.startKey,
	}, func(page *s3.ListObjectsV2Output, lastPage bool) bool {
		// TODO: parallelise this
		for _, object := range page.Contents {
			key := *object.Key
			downbuf := aws.NewWriteAtBuffer(nil)
			_, err := downloader.DownloadWithContext(ctx, downbuf, &s3.GetObjectInput{
				Bucket: &s.bucket,
				Key:    &key,
			})
			if err != nil {
				panic(err)
			}

			body := downbuf.Bytes()
			fmt.Printf("body len %d\n", len(body))

			err = appendChangeset(changeset, buf, body)
			if err != nil {
				panic(err)
			}
			if s.startKey == nil || key > *s.startKey {
				s.startKey = &key
			}
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
