package wtintpack

import (
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestLeadingZeros(t *testing.T) {
	cases := map[string]struct {
		x    uint64
		want byte
	}{
		"0":         {x: 0, want: 8},
		"1":         {x: 1, want: 7},
		"62746":     {x: 62746, want: 6},
		"5639001":   {x: 5639001, want: 5},
		"838251350": {x: 838251350, want: 4},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			var b byte

			b = wtLeadingZeros(tc.x, b)

			if diff := cmp.Diff(tc.want, b); diff != "" {
				t.Fatalf("output doesn't match (-want +got):\n%s", diff)
			}
		})
	}
}

func TestPackUint(t *testing.T) {
	cases := []struct {
		x      uint64
		packed []byte
	}{
		{x: 0, packed: []byte{0x80}},
		{x: 1, packed: []byte{0x81}},
		{x: 50, packed: []byte{0xb2}},
		{x: 63, packed: []byte{0xbf}},
		{x: 64, packed: []byte{0xc0, 0x00}},
		{x: 100, packed: []byte{0xc0, 0x24}},
		{x: 8255, packed: []byte{0xdf, 0xff}},
		{x: 8256, packed: []byte{0xe1, 0x00}},
		{x: 8257, packed: []byte{0xe1, 0x01}},
		{x: 9999, packed: []byte{0xe2, 0x06, 0xcf}},
		{x: 99999, packed: []byte{0xe3, 0x01, 0x66, 0x5f}},
		{x: 999999, packed: []byte{0xe3, 0x0f, 0x21, 0xff}},
		{x: 99999999, packed: []byte{0xe4, 0x05, 0xf5, 0xc0, 0xbf}},
		{x: 999999999, packed: []byte{0xe4, 0x3b, 0x9a, 0xa9, 0xbf}},
		{x: 9999999999, packed: []byte{0xe5, 0x02, 0x54, 0x0b, 0xc3, 0xbf}},
	}

	buf := make([]byte, 0, 100)
	unpack := make([]byte, 0, 100)

	for _, tc := range cases {
		t.Run(fmt.Sprintf("%d", tc.x), func(t *testing.T) {
			buf = buf[:0]
			unpack = unpack[:0]

			buf = PackUint(buf, tc.x)

			if diff := cmp.Diff(tc.packed, buf); diff != "" {
				t.Fatalf("output doesn't match (-want +got):\n%s", diff)
			}

			unpack := append(buf, buf...)

			u, x := UnpackUint(unpack)

			if diff := cmp.Diff(tc.x, x); diff != "" {
				t.Fatalf("UnpackUint() integer doesn't match (-want +got):\n%s", diff)
			}

			if diff := cmp.Diff(buf, u); diff != "" {
				t.Fatalf("UnpackUint() remainder doesn't match (-want +got):\n%s", diff)
			}
		})
	}

	t.Run("random", func(t *testing.T) {
		for _, i := range []int64{100, 10000, 1 << 40, math.MaxInt64} {
			for j := 0; 1000 > j; j++ {
				buf = buf[:0]
				n := rand.Int63n(i)

				buf = PackUint(buf, uint64(n))
				b, x := UnpackUint(buf)
				buf = b

				if diff := cmp.Diff(uint64(n), x); diff != "" {
					t.Fatalf("UnpackUint(%d) didn't match PackUint() (-want +got):\n%s", n, diff)
				}
			}
		}
	})
}

func TestPackInt(t *testing.T) {
	cases := []struct {
		x      int64
		packed []byte
	}{
		{x: -99999999, packed: []byte{0x14, 0xfa, 0x0a, 0x1f, 0x01}},
		{x: -9999999, packed: []byte{0x15, 0x67, 0x69, 0x81}},
		{x: -999999, packed: []byte{0x15, 0xf0, 0xbd, 0xc1}},
		{x: -99999, packed: []byte{0x15, 0xfe, 0x79, 0x61}},
		{x: -9999, packed: []byte{0x16, 0xd8, 0xf1}},
		{x: -8257, packed: []byte{0x16, 0xdf, 0xbf}},
		{x: -8256, packed: []byte{0x20, 0x00}},
		{x: -100, packed: []byte{0x3f, 0xdc}},
		{x: -65, packed: []byte{0x3f, 0xff}},
		{x: -64, packed: []byte{0x40}},
		{x: -50, packed: []byte{0x4e}},
		{x: -1, packed: []byte{0x7f}},
		{x: 0, packed: []byte{0x80}},
		{x: 1, packed: []byte{0x81}},
		{x: 50, packed: []byte{0xb2}},
		{x: 63, packed: []byte{0xbf}},
		{x: 64, packed: []byte{0xc0, 0x00}},
		{x: 100, packed: []byte{0xc0, 0x24}},
		{x: 8255, packed: []byte{0xdf, 0xff}},
		{x: 8256, packed: []byte{0xe1, 0x00}},
		{x: 8257, packed: []byte{0xe1, 0x01}},
		{x: 9999, packed: []byte{0xe2, 0x06, 0xcf}},
		{x: 99999, packed: []byte{0xe3, 0x01, 0x66, 0x5f}},
		{x: 999999, packed: []byte{0xe3, 0x0f, 0x21, 0xff}},
		{x: 99999999, packed: []byte{0xe4, 0x05, 0xf5, 0xc0, 0xbf}},
		{x: 999999999, packed: []byte{0xe4, 0x3b, 0x9a, 0xa9, 0xbf}},
		{x: 9999999999, packed: []byte{0xe5, 0x02, 0x54, 0x0b, 0xc3, 0xbf}},
	}

	buf := make([]byte, 0, 100)
	unpack := make([]byte, 0, 100)

	for _, tc := range cases {
		t.Run(fmt.Sprintf("%d", tc.x), func(t *testing.T) {
			buf = buf[:0]
			unpack = unpack[:0]

			buf = PackInt(buf, tc.x)

			if diff := cmp.Diff(tc.packed, buf); diff != "" {
				t.Fatalf("PackInt() doesn't match (-want +got):\n%s", diff)
			}

			unpack := append(buf, buf...)

			u, x := UnpackInt(unpack)

			if diff := cmp.Diff(tc.x, x); diff != "" {
				t.Fatalf("UnpackInt() integer doesn't match (-want +got):\n%s", diff)
			}

			if diff := cmp.Diff(buf, u); diff != "" {
				t.Fatalf("UnpackInt() remainder doesn't match (-want +got):\n%s", diff)
			}
		})
	}

	t.Run("random", func(t *testing.T) {
		for _, i := range []int64{100, 10000, 1 << 40, math.MaxInt64} {
			for j := 0; 1000 > j; j++ {
				buf = buf[:0]
				n := rand.Int63n(i)

				if j%2 == 0 {
					n = -n
				}

				buf = PackInt(buf, n)
				b, x := UnpackInt(buf)
				buf = b

				if diff := cmp.Diff(n, x); diff != "" {
					t.Fatalf("UnpackInt(%d) didn't match PackInt() (-want +got):\n%s", n, diff)
				}
			}
		}
	})
}
