package wtgo_test

import (
	"errors"
	"fmt"
	"os"
	"testing"
	"wtgo"

	"github.com/google/go-cmp/cmp"
)

func TestAll(t *testing.T) {
	tablename := "table:test-table"
	tableconf := "key_format=SS,value_format=SS"

	env, err := newTableCursorTestEnv("create", "", tablename, tableconf, "")
	if err != nil {
		t.Fatalf("new table cursor test env: %s", err)
	}

	t.Cleanup(func() { env.Close() })

	key1, key2 := "key-part-1", "key-part-2"
	value1, value2 := "value-part-1", "value-part-2"

	if err := env.cursor.SetKey(key1, key2); err != nil {
		t.Fatalf("set key: %s", err)
	}

	if err := env.cursor.SetValue(value1, value2); err != nil {
		t.Fatalf("set value: %s", err)
	}

	if err := env.cursor.Insert(); err != nil {
		t.Fatalf("insert: %s", err)
	}

	if err := env.cursor.Reset(); err != nil {
		t.Fatalf("reset: %s", err)
	}

	for env.cursor.Next() {
		var k1, k2, v1, v2 string

		if err := env.cursor.GetKey(&k1, &k2); err != nil {
			t.Fatalf("get key: %s", err)
		}

		if err := env.cursor.GetValue(&v1, &v2); err != nil {
			t.Fatalf("get value: %s", err)
		}

		if k1 != key1 {
			t.Fatalf("got key1 %s, wanted %s", k1, key1)
		}

		if k2 != key2 {
			t.Fatalf("got key2 %s, wanted %s", k2, key2)
		}

		if v1 != value1 {
			t.Fatalf("got value1 %s, wanted %s", v1, value1)
		}

		if v2 != value2 {
			t.Fatalf("got value2 %s, wanted %s", v2, value2)
		}
	}

	if err := env.cursor.Err(); err != nil {
		t.Fatalf("cursor: %s", err)
	}

	if err := env.conn.Close(""); err != nil {
		t.Fatalf("close: %s", err)
	}
}

type tableCursorTestEnv struct {
	conn    *wtgo.Connection
	session *wtgo.Session
	cursor  *wtgo.Cursor
	dir     string
}

func (env tableCursorTestEnv) Close() error {
	return os.RemoveAll(env.dir)
}

func newTableCursorTestEnv(connectionc, sessionc, tablename, tablec, cursorc string) (*tableCursorTestEnv, error) {
	dir, err := os.MkdirTemp(os.TempDir(), "test-search-*")
	if err != nil {
		return nil, fmt.Errorf("make temp dir: %w", err)
	}

	conn, err := wtgo.Open(dir, connectionc)
	if err != nil {
		os.RemoveAll(dir)
		return nil, fmt.Errorf("open database: %s", err)
	}

	session, err := conn.OpenSession(sessionc)
	if err != nil {
		os.RemoveAll(dir)
		return nil, fmt.Errorf("open session: %s", err)
	}

	if err := session.Create(tablename, tablec); err != nil {
		os.RemoveAll(dir)
		return nil, fmt.Errorf("create object: %s", err)
	}

	cursor, err := session.OpenCursor(tablename, cursorc)
	if err != nil {
		os.RemoveAll(dir)
		return nil, fmt.Errorf("open cursor: %s", err)
	}

	env := &tableCursorTestEnv{
		conn:    conn,
		session: session,
		cursor:  cursor,
		dir:     dir,
	}

	return env, nil
}

func TestRemove(t *testing.T) {
	tablename := "table:test-table"
	tableconf := "key_format=S,value_format=S"

	env, err := newTableCursorTestEnv("create", "", tablename, tableconf, "")
	if err != nil {
		t.Fatalf("new table cursor test env: %s", err)
	}

	t.Cleanup(func() { env.Close() })

	records := []record{
		{k: []any{"1"}, v: []any{"a"}},
		{k: []any{"2"}, v: []any{"b"}},
		{k: []any{"3"}, v: []any{"c"}},
	}

	if err := seed(env.cursor, records); err != nil {
		t.Fatalf("seed database: %s", err)
	}

	if err := env.cursor.SetKey("1"); err != nil {
		t.Fatalf("set remove key: %s", err)
	}

	if err := env.cursor.Remove(); err != nil {
		t.Fatalf("remove: %s", err)
	}

	if err := env.cursor.SetKey("1"); err != nil {
		t.Fatalf("set search key: %s", err)
	}

	if err := env.cursor.Search(); !errors.Is(err, wtgo.ErrNotFound) {
		t.Fatalf("search for missing key returned err '%s', expected not found", err)
	}
}

func TestSearch(t *testing.T) {
	tablename := "table:test-table"
	tableconf := "key_format=s,value_format=s"

	env, err := newTableCursorTestEnv("create", "", tablename, tableconf, "")
	if err != nil {
		t.Fatalf("new table cursor test env: %s", err)
	}

	t.Cleanup(func() { env.Close() })

	const alpha = "abcdefghijklmnopqrstuvwxyz"

	for _, char := range alpha {
		if err := env.cursor.SetKey(string(char)); err != nil {
			t.Fatalf("seed database set key: %s", err)
		}

		if err := env.cursor.SetValue(string(char)); err != nil {
			t.Fatalf("seed database set value: %s", err)
		}

		if err := env.cursor.Insert(); err != nil {
			t.Fatalf("seed database insert: %s", err)
		}
	}

	for _, char := range alpha {
		key := string(char)
		value := key

		if err := env.cursor.Reset(); err != nil {
			t.Fatalf("reset: %s", err)
		}

		if err := env.cursor.SetKey(key); err != nil {
			t.Fatalf("set key: %s", err)
		}

		if err := env.cursor.Search(); err != nil {
			t.Fatalf("search: %s", err)
		}

		var k, v string

		if err := env.cursor.GetKey(&k); err != nil {
			t.Fatalf("get key: %s", err)
		}

		if err := env.cursor.GetValue(&v); err != nil {
			t.Fatalf("get value: %s", err)
		}

		if diff := cmp.Diff(key, k); diff != "" {
			t.Fatalf("found key doesn't match (-want +got):\n%s", diff)
		}

		if diff := cmp.Diff(value, v); diff != "" {
			t.Fatalf("found value doesn't match (-want +got):\n%s", diff)
		}
	}

	if err := env.cursor.Reset(); err != nil {
		t.Fatalf("reset: %s", err)
	}

	if err := env.cursor.SetKey("A"); err != nil {
		t.Fatalf("set key: %s", err)
	}

	if err := env.cursor.Search(); !errors.Is(err, wtgo.ErrNotFound) {
		t.Fatalf("search for missing key returned err '%s', expected not found", err)
	}
}

func insert[K, V any](cursor *wtgo.Cursor, k K, v V) error {
	if err := cursor.Reset(); err != nil {
		return fmt.Errorf("reset: %w", err)
	}

	if err := cursor.SetKey(k); err != nil {
		return fmt.Errorf("set key: %s", err)
	}

	if err := cursor.SetValue(v); err != nil {
		return fmt.Errorf("set value: %s", err)
	}

	if err := cursor.Insert(); err != nil {
		return fmt.Errorf("insert: %w", err)
	}

	return nil
}

type result[K, V any] struct {
	key   K
	value V
}

func searchKey[K, V any](cursor *wtgo.Cursor, key K) (*result[K, V], error) {
	if err := cursor.Reset(); err != nil {
		return nil, fmt.Errorf("reset: %w", err)
	}

	if err := cursor.SetKey(key); err != nil {
		return nil, fmt.Errorf("set key: %w", err)
	}

	if err := cursor.Search(); err != nil {
		return nil, fmt.Errorf("search: %w", err)
	}

	var k K

	if err := cursor.GetKey(&k); err != nil {
		return nil, fmt.Errorf("get key: %w", err)
	}

	var v V

	if err := cursor.GetValue(&v); err != nil {
		return nil, fmt.Errorf("get value: %w", err)
	}

	r := &result[K, V]{
		key:   k,
		value: v,
	}

	return r, nil
}

type record struct {
	k []any
	v []any
}

func seed(cursor *wtgo.Cursor, data []record) error {
	for _, d := range data {
		if err := cursor.SetKey(d.k...); err != nil {
			return fmt.Errorf("set key: %w", err)
		}

		if err := cursor.SetValue(d.v...); err != nil {
			return fmt.Errorf("set value: %w", err)
		}

		if err := cursor.Insert(); err != nil {
			return fmt.Errorf("insert: %w", err)
		}

		if err := cursor.Reset(); err != nil {
			return fmt.Errorf("reset: %w", err)
		}
	}

	return nil
}

func TestPrev(t *testing.T) {
	tablename := "table:test-table"
	tableconf := "key_format=S,value_format=S"

	env, err := newTableCursorTestEnv("create", "", tablename, tableconf, "")
	if err != nil {
		t.Fatalf("new table cursor test env: %s", err)
	}

	t.Cleanup(func() { env.Close() })

	records := []record{
		{k: []any{"1"}, v: []any{"a"}},
		{k: []any{"2"}, v: []any{"b"}},
		{k: []any{"3"}, v: []any{"c"}},
	}

	if err := seed(env.cursor, records); err != nil {
		t.Fatalf("seed database: %s", err)
	}

	for i := len(records) - 1; env.cursor.Prev(); i-- {
		var k, v string

		if err := env.cursor.GetKey(&k); err != nil {
			t.Fatalf("get key: %s", err)
		}

		if err := env.cursor.GetValue(&v); err != nil {
			t.Fatalf("get value: %s", err)
		}

		want := records[i]

		if diff := cmp.Diff(want.k, []any{k}); diff != "" {
			t.Fatalf("key doesn't match (-want +got):\n%s", diff)
		}

		if diff := cmp.Diff(want.v, []any{v}); diff != "" {
			t.Fatalf("value doesn't match (-want +got):\n%s", diff)
		}
	}

	if err := env.cursor.Err(); err != nil {
		t.Fatalf("cursor: %s", err)
	}
}

func TestModify(t *testing.T) {
	tablename := "table:test-table"
	tableconf := "key_format=S,value_format=S"

	env, err := newTableCursorTestEnv("create", "", tablename, tableconf, "")
	if err != nil {
		t.Fatalf("new table cursor test env: %s", err)
	}

	t.Cleanup(func() { env.Close() })

	records := []record{
		{k: []any{"1"}, v: []any{"a"}},
		{k: []any{"2"}, v: []any{"b"}},
		{k: []any{"3"}, v: []any{"c"}},
	}

	if err := seed(env.cursor, records); err != nil {
		t.Fatalf("seed database: %s", err)
	}

	if err := env.cursor.Reset(); err != nil {
		t.Fatalf("reset cursor: %s", err)
	}

	modifications := []wtgo.Modification{
		{
			Data:   []byte("hello world"),
			Offset: 0,
			Size:   1,
		},
	}

	if err := env.session.BeginTransaction(""); err != nil {
		t.Fatalf("begin transaction: %s", err)
	}

	if err := env.cursor.SetKey("1"); err != nil {
		t.Fatalf("set key: %s", err)
	}

	if err := env.cursor.Search(); err != nil {
		t.Fatalf("search: %s", err)
	}

	if err := env.cursor.Modify(modifications); err != nil {
		t.Fatalf("modify: %s", err)
	}

	if err := env.session.CommitTransaction(""); err != nil {
		t.Fatalf("commit transaction: %s", err)
	}

	if err := env.cursor.Reset(); err != nil {
		t.Fatalf("reset cursor after commit: %s", err)
	}

	for i := 0; env.cursor.Next(); i++ {
		var k, v string

		if err := env.cursor.GetKey(&k); err != nil {
			t.Fatalf("get key: %s", err)
		}

		if err := env.cursor.GetValue(&v); err != nil {
			t.Fatalf("get value: %s", err)
		}

		want := records[i]
		if i == 0 {
			want.v = []any{"hello world"}
		}

		if diff := cmp.Diff(want.k, []any{k}); diff != "" {
			t.Fatalf("key doesn't match (-want +got):\n%s", diff)
		}

		if diff := cmp.Diff(want.v, []any{v}); diff != "" {
			t.Fatalf("value doesn't match (-want +got):\n%s", diff)
		}
	}

	if err := env.cursor.Err(); err != nil {
		t.Fatalf("iteration: %s", err)
	}
}

func TestTransactions(t *testing.T) {
	tablename := "table:test-table"
	tableconf := "key_format=S,value_format=S"

	env, err := newTableCursorTestEnv("create", "", tablename, tableconf, "")
	if err != nil {
		t.Fatalf("new table cursor test env: %s", err)
	}

	t.Cleanup(func() { env.Close() })

	if err := env.session.BeginTransaction(""); err != nil {
		t.Fatalf("begin transaction: %s", err)
	}

	if err := insert(env.cursor, "a", "a"); err != nil {
		t.Fatalf("insert after first begin: %s", err)
	}

	if err := env.session.RollbackTransaction(""); err != nil {
		t.Fatalf("rollback transaction: %s", err)
	}

	if _, err := searchKey[string, string](env.cursor, "a"); !errors.Is(err, wtgo.ErrNotFound) {
		t.Fatalf("search for missing key returned err '%s', expected not found", err)
	}

	if err := env.session.BeginTransaction("sync=true"); err != nil {
		t.Fatalf("begin transaction: %s", err)
	}

	if err := insert(env.cursor, "b", "b"); err != nil {
		t.Fatalf("insert after second begin: %s", err)
	}

	if err := env.session.CommitTransaction("sync=on"); err != nil {
		t.Fatalf("commit transaction: %s", err)
	}

	if _, err := searchKey[string, string](env.cursor, "b"); err != nil {
		t.Fatalf("search key after commit: %s", err)
	}

	if err := env.conn.Close(""); err != nil {
		t.Fatalf("close connection: %s", err)
	}
}

func TestCursorOperations(t *testing.T) {
	tablename := "table:test-table"
	tableconf := "key_format=S,value_format=S"

	env, err := newTableCursorTestEnv("create", "", tablename, tableconf, "")
	if err != nil {
		t.Fatalf("new table cursor test env: %s", err)
	}

	t.Cleanup(func() { env.Close() })

	t.Run("compare", func(t *testing.T) {
		cases := map[string]struct {
			cursorKey string
			otherKey  string
			want      wtgo.CursorComparison
		}{
			"equal": {
				cursorKey: "a",
				otherKey:  "a",
				want:      wtgo.CursorComparisonEqual,
			},
			"less-than": {
				cursorKey: "a",
				otherKey:  "b",
				want:      wtgo.CursorComparisonLessThan,
			},
			"greater-than": {
				cursorKey: "b",
				otherKey:  "a",
				want:      wtgo.CursorComparisonGreaterThan,
			},
		}

		other, err := env.session.OpenCursor(tablename, "")
		if err != nil {
			t.Fatalf("open other: %s", err)
		}

		for name, tc := range cases {
			t.Run(name, func(t *testing.T) {
				if err := env.cursor.Reset(); err != nil {
					t.Fatalf("reset: %s", err)
				}

				if err := other.Reset(); err != nil {
					t.Fatalf("reset: %s", err)
				}

				if err := env.cursor.SetKey(tc.cursorKey); err != nil {
					t.Fatalf("set cursor key: %s", err)
				}

				if err := other.SetKey(tc.otherKey); err != nil {
					t.Fatalf("set other key: %s", err)
				}

				comparison, err := env.cursor.Compare(other)
				if err != nil {
					t.Fatalf("compare: %s", err)
				}

				if diff := cmp.Diff(tc.want, comparison); diff != "" {
					t.Fatalf("output doesn't match (-want +got):\n%s", diff)
				}
			})
		}
	})
}
