package pgxrows

import (
	"io"

	"github.com/jackc/pgx/v5"
	"github.com/sharpvik/fungi"
)

type stream[T any] struct {
	rows pgx.Rows
	next func(pgx.Rows) (T, error)
}

func Stream[T any](
	rows pgx.Rows,
	next func(pgx.Rows) (T, error),
) fungi.Stream[T] {
	return &stream[T]{
		rows: rows,
		next: next,
	}
}

func (s *stream[T]) Next() (t T, err error) {
	defer func() {
		if err != nil {
			s.rows.Close()
		}
	}()

	if s.rows.Next() {
		t, err = s.next(s.rows)
	} else {
		err = io.EOF
	}

	return
}
