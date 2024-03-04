package wtformat_test

import (
	"testing"
	"wtgo/internal/wtformat"

	"github.com/google/go-cmp/cmp"
)

func TestParseFormat(t *testing.T) {
	cases := map[string]struct {
		format string
		data   []any
		err    error
		buf    []byte
	}{
		"parse-string-null-without-null": {
			format: "S",
			err:    nil,
			data:   []any{"hello"},
			buf:    []byte("hello\000"),
		},
		"parse-string-null-with-null": {
			format: "S",
			err:    nil,
			data:   []any{"hello\000"},
			buf:    []byte("hello\000"),
		},
		"parse-string-null-size-exact-without-null": {
			format: "5S",
			err:    nil,
			data:   []any{"hello"},
			buf:    []byte("hello\000"),
		},
		"parse-string-null-size-exact-with-null": {
			format: "6S",
			err:    nil,
			data:   []any{"hello\000"},
			buf:    []byte("hello\000"),
		},
		"parse-string-null-size-less-than": {
			format: "3S",
			err:    nil,
			data:   []any{"hello"},
			buf:    []byte("hello"),
		},
		"parse-string-null-size-greater-than": {
			format: "7S",
			err:    nil,
			data:   []any{"hello"},
			buf:    []byte("hello\000\000"),
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			packers, err := wtformat.ParseFormat(tc.format)
			if err != tc.err {
				t.Fatalf("expected error '%s', got '%s'", tc.err, err)
			}

			if len(packers) != len(tc.data) {
				t.Fatalf("got %d packers, expected %d", len(packers), len(tc.data))
			}

			got := make([]byte, 0, len(tc.buf))

			for i, p := range packers {
				g, err := p.PackField(tc.data[i], got)
				if err != nil {
					t.Fatalf("pack field: %s", err)
				}

				got = g
			}

			if diff := cmp.Diff(tc.buf, got); diff != "" {
				t.Errorf("packed buffer doesn't match (-want +got):\n%s", diff)
			}
		})
	}
}
