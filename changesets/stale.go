package changesets

import (
	"context"
	"fmt"
	"io"
	"time"
)

type staleChangesets struct {
	Reader
	lastRefreshed time.Time
	window        time.Duration
}

func (s *staleChangesets) GetChangeset(ctx context.Context) (io.Reader, error) {
	if s.lastRefreshed.Add(s.window).Before(time.Now()) {
		fmt.Println("refreshing")
		s.lastRefreshed = time.Now()

		return s.Reader.GetChangeset(ctx)
	}

	return nil, nil
}
