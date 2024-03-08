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

func uint8VarPtr() *uint8 {
	var i uint8
	return &i
}

func int8VarPtr() *int8 {
	var i int8
	return &i
}

func uint16VarPtr() *uint16 {
	var i uint16
	return &i
}

func int16VarPtr() *int16 {
	var i int16
	return &i
}

func uint32VarPtr() *uint32 {
	var i uint32
	return &i
}

func int32VarPtr() *int32 {
	var i int32
	return &i
}

func uint64VarPtr() *uint64 {
	var i uint64
	return &i
}

func int64VarPtr() *int64 {
	var i int64
	return &i
}

func byteVarPtr() *byte {
	b := byte(0)
	return &b
}

func byteSliceVarPtr() *[]byte {
	b := make([]byte, 0)
	return &b
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
			packed: []byte("hello\x00"),
			output: []any{"hello"},
			vars:   []any{strVarPtr()},
		},
		"parse-string-null-with-null": {
			format: "S",
			err:    nil,
			input:  []any{"hello\x00"},
			packed: []byte("hello\x00"),
			output: []any{"hello"},
			vars:   []any{strVarPtr()},
		},
		"parse-string-null-size-exact-without-null": {
			format: "5S",
			err:    nil,
			input:  []any{"hello"},
			packed: []byte("hello\x00"),
			output: []any{"hello"},
			vars:   []any{strVarPtr()},
		},
		"parse-string-null-size-exact-with-null": {
			format: "6S",
			err:    nil,
			input:  []any{"hello\x00"},
			packed: []byte("hello\x00"),
			output: []any{"hello\x00"},
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
			packed: []byte("hello\x00\x00"),
			output: []any{"hello\x00\x00"},
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
			packed: []byte("hello\x00\x00"),
			output: []any{"hello\x00\x00"},
			vars:   []any{strVarPtr()},
		},
		"parse-unsigned-int-8": {
			format: "B",
			input:  []any{uint8(42)},
			err:    nil,
			packed: []byte{170},
			output: []any{uint8(42)},
			vars:   []any{uint8VarPtr()},
		},
		"parse-signed-int-8": {
			format: "b",
			input:  []any{int8(42)},
			err:    nil,
			packed: []byte{170},
			output: []any{int8(42)},
			vars:   []any{int8VarPtr()},
		},
		"parse-unsigned-int-16": {
			format: "H",
			input:  []any{uint16(42)},
			err:    nil,
			packed: []byte{170},
			output: []any{uint16(42)},
			vars:   []any{uint16VarPtr()},
		},
		"parse-signed-int-16": {
			format: "h",
			input:  []any{int16(42)},
			err:    nil,
			packed: []byte{170},
			output: []any{int16(42)},
			vars:   []any{int16VarPtr()},
		},
		"parse-unsigned-int-32-I": {
			format: "I",
			input:  []any{uint32(42)},
			err:    nil,
			packed: []byte{170},
			output: []any{uint32(42)},
			vars:   []any{uint32VarPtr()},
		},
		"parse-signed-int-32-i": {
			format: "i",
			input:  []any{int32(42)},
			err:    nil,
			packed: []byte{170},
			output: []any{int32(42)},
			vars:   []any{int32VarPtr()},
		},
		"parse-unsigned-int-32-L": {
			format: "L",
			input:  []any{uint32(42)},
			err:    nil,
			packed: []byte{170},
			output: []any{uint32(42)},
			vars:   []any{uint32VarPtr()},
		},
		"parse-signed-int-32-l": {
			format: "l",
			input:  []any{int32(42)},
			err:    nil,
			packed: []byte{170},
			output: []any{int32(42)},
			vars:   []any{int32VarPtr()},
		},
		"parse-unsigned-int-64": {
			format: "Q",
			input:  []any{uint64(42)},
			err:    nil,
			packed: []byte{170},
			output: []any{uint64(42)},
			vars:   []any{uint64VarPtr()},
		},
		"parse-signed-int-64": {
			format: "q",
			input:  []any{int64(42)},
			err:    nil,
			packed: []byte{170},
			output: []any{int64(42)},
			vars:   []any{int64VarPtr()},
		},
		"parse-record": {
			format: "r",
			input:  []any{uint64(42)},
			err:    nil,
			packed: []byte{170},
			output: []any{uint64(42)},
			vars:   []any{uint64VarPtr()},
		},
		// tests prefixed "wt-" came directly from test_pack.py in the WiredTiger
		// codebase and they should not be altered
		"wt-1": {
			format: "10SS",
			input:  []any{"aaaaa\x00\x00\x00\x00\x00", "something"},
			err:    nil,
			packed: []byte("aaaaa\x00\x00\x00\x00\x00something\x00"),
			output: []any{"aaaaa\x00\x00\x00\x00\x00", "something"},
			vars:   []any{strVarPtr(), strVarPtr()},
		},

		"wt-2": {
			format: "S10S",
			input:  []any{"something", "aaaaa\x00\x00\x00\x00\x00"},
			err:    nil,
			packed: []byte("something\x00aaaaa\x00\x00\x00\x00\x00"),
			output: []any{"something", "aaaaa\x00\x00\x00\x00\x00"},
			vars:   []any{strVarPtr(), strVarPtr()},
		},
		"wt-3": {
			format: "S",
			input:  []any{"abc"},
			err:    nil,
			packed: []byte("abc\x00"),
			output: []any{"abc"},
			vars:   []any{strVarPtr()},
		},
		"wt-4": {
			format: "9S",
			input:  []any{"aaaaaaaaa"},
			err:    nil,
			packed: []byte("aaaaaaaaa\x00"),
			output: []any{"aaaaaaaaa"},
			vars:   []any{strVarPtr()},
		},
		"wt-5": {
			format: "9SS",
			input:  []any{"forty two", "spam egg"},
			err:    nil,
			packed: []byte("forty two\x00spam egg\x00"),
			output: []any{"forty two", "spam egg"},
			vars:   []any{strVarPtr(), strVarPtr()},
		},
		"wt-6": {
			format: "42S",
			input:  []any{"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
			err:    nil,
			packed: []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\x00"),
			output: []any{"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
			vars:   []any{strVarPtr()},
		},
		"wt-7": {
			format: "42SS",
			input:  []any{"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "something"},
			err:    nil,
			packed: []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\x00something\x00"),
			output: []any{"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "something"},
			vars:   []any{strVarPtr(), strVarPtr()},
		},
		"wt-8": {
			format: "S42S",
			input:  []any{"something", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
			err:    nil,
			packed: []byte("something\x00aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\x00"),
			output: []any{"something", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
			vars:   []any{strVarPtr(), strVarPtr()},
		},
		"wt-9": {
			format: "s",
			input:  []any{"4"},
			err:    nil,
			packed: []byte("4"),
			output: []any{"4"},
			vars:   []any{strVarPtr()},
		},
		"wt-10": {
			format: "1s",
			input:  []any{"4"},
			err:    nil,
			packed: []byte("4"),
			output: []any{"4"},
			vars:   []any{strVarPtr()},
		},
		"wt-11": {
			format: "2s",
			input:  []any{"42"},
			err:    nil,
			packed: []byte("42"),
			output: []any{"42"},
			vars:   []any{strVarPtr()},
		},
		"wt-12": {
			format: "iii",
			input:  []any{int32(0), int32(101), int32(-99)},
			err:    nil,
			packed: []byte{0x80, 0xc0, 0x25, 0x3F, 0xdd},
			output: []any{int32(0), int32(101), int32(-99)},
			vars:   []any{int32VarPtr(), int32VarPtr(), int32VarPtr()},
		},
		"wt-13": {
			format: "3i",
			input:  []any{int32(0), int32(101), int32(-99)},
			err:    nil,
			packed: []byte{0x80, 0xc0, 0x25, 0x3F, 0xdd},
			output: []any{int32(0), int32(101), int32(-99)},
			vars:   []any{int32VarPtr(), int32VarPtr(), int32VarPtr()},
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
				t.Fatalf("output doesn't match (-want +got):\n%s", diff)
			}
		})
	}
}
