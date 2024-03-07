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
	conn, err := wtgo.Open("./test", "create")
	if err != nil {
		t.Fatalf("open database: %s", err)
	}

	session, err := conn.OpenSession("")
	if err != nil {
		t.Fatalf("open session: %s", err)
	}

	if err := session.Create("table:test-table", "key_format=SS,value_format=SS"); err != nil {
		t.Fatalf("create object: %s", err)
	}

	cursor, err := session.OpenCursor("table:test-table", "")
	if err != nil {
		t.Fatalf("open cursor: %s", err)
	}

	key1, key2 := "key-part-1", "key-part-2"
	value1, value2 := "value-part-1", "value-part-2"

	if err := cursor.SetKey(key1, key2); err != nil {
		t.Fatalf("set key: %s", err)
	}

	if err := cursor.SetValue(value1, value2); err != nil {
		t.Fatalf("set value: %s", err)
	}

	if err := cursor.Insert(); err != nil {
		t.Fatalf("insert: %s", err)
	}

	if err := cursor.Reset(); err != nil {
		t.Fatalf("reset: %s", err)
	}

	for cursor.Next() {
		var k1, k2, v1, v2 string

		if err := cursor.GetKey(&k1, &k2); err != nil {
			t.Fatalf("get key: %s", err)
		}

		if err := cursor.GetValue(&v1, &v2); err != nil {
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

	if err := cursor.Err(); err != nil {
		t.Fatalf("cursor: %s", err)
	}

	if err := conn.Close(""); err != nil {
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
