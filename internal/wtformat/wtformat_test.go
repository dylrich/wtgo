package wtformat_test

import (
	"reflect"
	"testing"
	"wtgo/internal/wtformat"

	"github.com/google/go-cmp/cmp"
)

func strVarPtr() *string {
	s := ""
	return &s
}

func TestParseFormat(t *testing.T) {
	cases := map[string]struct {
		format string
		input  []any
		err    error
		packed []byte
		output []any
		vars   []any
	}{
		"parse-string-null-without-null": {
			format: "S",
			err:    nil,
			input:  []any{"hello"},
			packed: []byte("hello\000"),
			output: []any{"hello"},
			vars:   []any{strVarPtr()},
		},
		"parse-string-null-with-null": {
			format: "S",
			err:    nil,
			input:  []any{"hello\000"},
			packed: []byte("hello\000"),
			output: []any{"hello"},
			vars:   []any{strVarPtr()},
		},
		"parse-string-null-size-exact-without-null": {
			format: "5S",
			err:    nil,
			input:  []any{"hello"},
			packed: []byte("hello\000"),
			output: []any{"hello"},
			vars:   []any{strVarPtr()},
		},
		"parse-string-null-size-exact-with-null": {
			format: "6S",
			err:    nil,
			input:  []any{"hello\000"},
			packed: []byte("hello\000"),
			output: []any{"hello\000"},
			vars:   []any{strVarPtr()},
		},
		// TODO: I don't think this test case is correct
		// check into the behavior with S and s
		"parse-string-null-size-less-than": {
			format: "3S",
			err:    nil,
			input:  []any{"hello"},
			packed: []byte("hello"),
			output: []any{"hel"},
			vars:   []any{strVarPtr()},
		},
		"parse-string-null-size-greater-than": {
			format: "7S",
			err:    nil,
			input:  []any{"hello"},
			packed: []byte("hello\000\000"),
			output: []any{"hello\000\000"},
			vars:   []any{strVarPtr()},
		},
		"parse-string-fixed": {
			format: "s",
			err:    nil,
			input:  []any{"hello"},
			packed: []byte("h"),
			output: []any{"h"},
			vars:   []any{strVarPtr()},
		},
		"parse-string-fixed-size-exact": {
			format: "5s",
			err:    nil,
			input:  []any{"hello"},
			packed: []byte("hello"),
			output: []any{"hello"},
			vars:   []any{strVarPtr()},
		},
		"parse-string-fixed-size-less-than": {
			format: "3s",
			err:    nil,
			input:  []any{"hello"},
			packed: []byte("hel"),
			output: []any{"hel"},
			vars:   []any{strVarPtr()},
		},
		"parse-string-fixed-size-greater-than": {
			format: "7s",
			err:    nil,
			input:  []any{"hello"},
			packed: []byte("hello\000\000"),
			output: []any{"hello\000\000"},
			vars:   []any{strVarPtr()},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			packers, err := wtformat.ParseFormat(tc.format)
			if err != tc.err {
				t.Fatalf("expected error '%s', got '%s'", tc.err, err)
			}

			if len(packers) != len(tc.input) {
				t.Fatalf("got %d packers, expected %d", len(packers), len(tc.input))
			}

			gotPacked := make([]byte, 0, len(tc.packed))

			for i, p := range packers {
				g, err := p.PackField(tc.input[i], gotPacked)
				if err != nil {
					t.Fatalf("pack field: %s", err)
				}

				gotPacked = g
			}

			if diff := cmp.Diff(tc.packed, gotPacked); diff != "" {
				t.Fatalf("packed buffer doesn't match (-want +got):\n%s", diff)
			}

			for i, p := range packers {
				buf, err := p.UnpackField(gotPacked, tc.vars[i])
				if err != nil {
					t.Fatalf("unpack field: %s", err)
				}

				// so, so gross
				// The vars slice contains pointers in order to make it easy
				// to unpack arbitrary values into them. This makes it hard to
				// run cmp.Diff on the output and vars slices, because the
				// output should be concrete types. Instead, do some magic
				// reflection to change the vars slice to contain concrete types
				// after we're done unpacking
				v := reflect.ValueOf(tc.vars[i])
				tc.vars[i] = v.Elem().Interface()

				gotPacked = buf
			}

			if diff := cmp.Diff(tc.output, tc.vars); diff != "" {
				t.Fatalf("unpacking doesn't match (-want +got):\n%s", diff)
			}
		})
	}
}
