package wtgo_test

import (
	"testing"
	"wtgo"
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
