package wtgo

/*
#cgo CFLAGS: -g -Wall
#cgo LDFLAGS: -L. -lwiredtiger
#include "wiredtiger.h"
#include <stdlib.h>

#define WT_SIZE_ZERO (size_t)((size_t)(SIZE_MAX) >> 1)

int wiredtiger_connection_open_session(WT_CONNECTION *connection, WT_EVENT_HANDLER *event_handler,	const char *config, WT_SESSION **sessionp) {
	return connection->open_session(connection, event_handler, config, sessionp);
}

int wiredtiger_connection_close(WT_CONNECTION *connection, const char *config) {
	return connection->close(connection, config);
}

int wiredtiger_session_create(WT_SESSION *session, const char *name, const char *config ) {
	return session->create(session, name, config);
}

int wiredtiger_session_open_cursor(WT_SESSION *session, const char *uri, WT_CURSOR *to_dup, const char *config, WT_CURSOR **cursorp) {
	int ret = session->open_cursor(session, uri, to_dup, config, cursorp);
	if (ret != 0) {
		return ret;
	}

	(*cursorp)->flags |= WT_CURSTD_RAW;

	return 0;
}

int wiredtiger_cursor_insert(WT_CURSOR *cursor, const void *packed_key, size_t key_size, const void *packed_value, size_t value_size) {

	WT_ITEM key;
	key.data = packed_key;
	key.size = key_size;
	cursor->set_key(cursor, &key);

	WT_ITEM value;
	value.data = packed_value;
	value.size = value_size;
	cursor->set_value(cursor, &value);

	return cursor->insert(cursor);
}

int wiredtiger_cursor_reset(WT_CURSOR *cursor) {
	return cursor->reset(cursor);
}

int wiredtiger_cursor_get_key(WT_CURSOR *cursor, WT_ITEM *v) {
	return cursor->get_key(cursor, v);
}

int wiredtiger_cursor_get_value(WT_CURSOR *cursor, WT_ITEM *v) {
	return cursor->get_value(cursor, v);
}

int wiredtiger_cursor_next(WT_CURSOR *cursor) {
	return cursor->next(cursor);
}

int wiredtiger_cursor_prev(WT_CURSOR *cursor) {
	return cursor->prev(cursor);
}
*/
import (
	"C"
)
import (
	"fmt"
	"unsafe"
	"wtgo/internal/wtformat"
)

type Connection struct {
	wtc *C.WT_CONNECTION
}

func Open(home, config string) (*Connection, error) {
	var wtc *C.WT_CONNECTION

	homecstr := C.CString(home)
	configcstr := C.CString(config)

	code := int(C.wiredtiger_open(homecstr, nil, configcstr, &wtc))

	C.free(unsafe.Pointer(homecstr))
	C.free(unsafe.Pointer(configcstr))

	if code != 0 {
		return nil, ErrorCode(code)
	}

	conn := &Connection{
		wtc: wtc,
	}

	return conn, nil
}

type Session struct {
	wtsession *C.WT_SESSION
}

func (conn *Connection) OpenSession(config string) (*Session, error) {
	var wts *C.WT_SESSION

	var configcstr *C.char

	if config != "" {
		configcstr = C.CString(config)
		defer C.free(unsafe.Pointer(configcstr))
	}

	if code := int(C.wiredtiger_connection_open_session(conn.wtc, nil, configcstr, &wts)); code != 0 {
		return nil, ErrorCode(code)
	}

	s := &Session{
		wtsession: wts,
	}
	return s, nil
}

func (conn *Connection) Close(config string) error {
	var configcstr *C.char = nil

	if len(config) > 0 {
		configcstr = C.CString(config)
		defer C.free(unsafe.Pointer(configcstr))
	}

	if code := int(C.wiredtiger_connection_close(conn.wtc, configcstr)); code != 0 {
		return ErrorCode(code)
	}

	return nil
}

func (s *Session) Create(name, config string) error {
	namecstr := C.CString(name)
	configcstr := C.CString(config)

	code := int(C.wiredtiger_session_create(s.wtsession, namecstr, configcstr))

	C.free(unsafe.Pointer(namecstr))
	C.free(unsafe.Pointer(configcstr))

	if code != 0 {
		return ErrorCode(code)
	}

	return nil
}

type Cursor struct {
	wtcursor *C.WT_CURSOR

	keyPackers   []wtformat.FieldPacker
	valuePackers []wtformat.FieldPacker

	keybuf   []byte
	valuebuf []byte
	err      error
}

func (s *Session) OpenCursor(uri, config string) (*Cursor, error) {
	var wtcursor *C.WT_CURSOR
	var dup *C.WT_CURSOR

	uricstr := C.CString(uri)
	var configcstr *C.char

	if config != "" {
		configcstr = C.CString(config)
		defer C.free(unsafe.Pointer(configcstr))
	}

	code := int(C.wiredtiger_session_open_cursor(s.wtsession, uricstr, dup, configcstr, &wtcursor))

	C.free(unsafe.Pointer(uricstr))

	if code != 0 {
		return nil, ErrorCode(code)
	}

	keyFormat := C.GoString(wtcursor.key_format)
	keyPackers, err := wtformat.ParseFormat(keyFormat)
	if err != nil {
		return nil, fmt.Errorf("parse key format: %w", err)
	}

	valueFormat := C.GoString(wtcursor.value_format)
	valuePackers, err := wtformat.ParseFormat(valueFormat)
	if err != nil {
		return nil, fmt.Errorf("parse value format: %w", err)
	}

	cursor := &Cursor{
		wtcursor:     wtcursor,
		keyPackers:   keyPackers,
		valuePackers: valuePackers,
	}

	return cursor, nil
}

func (c *Cursor) SetKey(keys ...any) error {
	buf := c.keybuf

	if len(keys) != len(c.keyPackers) {
		return fmt.Errorf("number of keys does not match format")
	}

	for i, p := range c.keyPackers {
		b, err := p.PackField(keys[i], buf)
		if err != nil {
			return err
		}

		buf = b
	}

	c.keybuf = buf

	return nil
}

func (c *Cursor) SetValue(values ...any) error {
	buf := c.valuebuf

	if len(values) != len(c.valuePackers) {
		return fmt.Errorf("number of values does not match format")
	}

	for i, p := range c.valuePackers {
		b, err := p.PackField(values[i], buf)
		if err != nil {
			return err
		}

		buf = b
	}

	c.valuebuf = buf

	return nil
}

func (c *Cursor) Insert() error {
	packedKey := unsafe.Pointer(&c.keybuf[0])
	keySize := C.size_t(len(c.keybuf))

	packedValue := unsafe.Pointer(&c.valuebuf[0])
	valueSize := C.size_t(len(c.valuebuf))

	if code := int(C.wiredtiger_cursor_insert(c.wtcursor, packedKey, keySize, packedValue, valueSize)); code != 0 {
		return ErrorCode(code)
	}

	c.keybuf = c.keybuf[:0]
	c.valuebuf = c.valuebuf[:0]

	return nil
}

func (c *Cursor) Reset() error {
	c.keybuf = c.keybuf[:0]
	c.valuebuf = c.valuebuf[:0]
	c.err = nil

	if code := int(C.wiredtiger_cursor_reset(c.wtcursor)); code != 0 {
		return ErrorCode(code)
	}

	return nil
}

func (c *Cursor) Next() bool {
	if code := int(C.wiredtiger_cursor_next(c.wtcursor)); code != 0 {
		if ErrorCode(code) == ErrNotFound {
			return false
		}

		c.err = ErrorCode(code)
		return false
	}

	return true
}

func (c *Cursor) Err() error {
	return c.err
}

func (c *Cursor) GetKey(keys ...any) error {
	var item C.WT_ITEM

	if code := int(C.wiredtiger_cursor_get_key(c.wtcursor, &item)); code != 0 {
		return ErrorCode(code)
	}

	data := C.GoBytes(unsafe.Pointer(item.data), C.int(item.size))

	for i, p := range c.keyPackers {
		d, err := p.UnpackField(data, keys[i])
		if err != nil {
			return fmt.Errorf("unpack field: %w", err)
		}

		data = d
	}

	return nil
}

func (c *Cursor) GetValue(values ...any) error {
	var item C.WT_ITEM

	if code := int(C.wiredtiger_cursor_get_value(c.wtcursor, &item)); code != 0 {
		return ErrorCode(code)
	}

	data := C.GoBytes(unsafe.Pointer(item.data), C.int(item.size))

	for i, p := range c.valuePackers {
		d, err := p.UnpackField(data, values[i])
		if err != nil {
			return fmt.Errorf("unpack field: %w", err)
		}

		data = d
	}

	return nil
}

type ErrorCode int16

const (
	ErrRollback        ErrorCode = -31800
	ErrDuplicateKey    ErrorCode = -31801
	ErrError           ErrorCode = -31802
	ErrNotFound        ErrorCode = -31803
	ErrPanic           ErrorCode = -31804
	ErrRestart         ErrorCode = -31805
	ErrRunRecovery     ErrorCode = -31806
	ErrCacheFull       ErrorCode = -31807
	ErrPrepareConflict ErrorCode = -31808
	ErrTrySalvage      ErrorCode = -31809
)

func (err ErrorCode) Error() string {
	return C.GoString(C.wiredtiger_strerror(C.int(err)))
}