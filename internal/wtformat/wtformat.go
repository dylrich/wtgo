package wtformat

import (
	"bytes"
	"fmt"
	"github.com/dylrich/wtgo/internal/wtformat/internal/wtintpack"
	"strings"
)

type FieldPacker interface {
	PackField(data any, buf []byte) ([]byte, error)
	UnpackField(buf []byte, data any) ([]byte, error)
}


type fieldPackerInt8 struct {
}

func (p fieldPackerInt8) PackField(data any, buf []byte) ([]byte, error) {
	v, ok := data.(int8)
	if !ok {
		return nil, fmt.Errorf("expected int8, got %T", data)
	}

	buf = wtintpack.PackInt(buf, int64(v))

	return buf, nil
}

func (p fieldPackerInt8) UnpackField(buf []byte, data any) ([]byte, error) {
	switch v := data.(type) {
	case *int8:
		buf, x := wtintpack.UnpackInt(buf)
		*v = int8(x)
		return buf, nil
	case *any:
		buf, x := wtintpack.UnpackInt(buf)
		*v = int8(x)
		return buf, nil
	default:
		return nil, fmt.Errorf("cannot unpack field into type %T", v)
	}
}

type fieldPackerUint8 struct {
}

func (p fieldPackerUint8) PackField(data any, buf []byte) ([]byte, error) {
	v, ok := data.(uint8)
	if !ok {
		return nil, fmt.Errorf("expected uint8, got %T", data)
	}

	buf = wtintpack.PackUint(buf, uint64(v))

	return buf, nil
}

func (p fieldPackerUint8) UnpackField(buf []byte, data any) ([]byte, error) {
	switch v := data.(type) {
	case *uint8:
		buf, x := wtintpack.UnpackInt(buf)
		*v = uint8(x)
		return buf, nil
	case *any:
		buf, x := wtintpack.UnpackInt(buf)
		*v = uint8(x)
		return buf, nil
	default:
		return nil, fmt.Errorf("cannot unpack field into type %T", v)
	}
}

type fieldPackerInt16 struct {
}

func (p fieldPackerInt16) PackField(data any, buf []byte) ([]byte, error) {
	v, ok := data.(int16)
	if !ok {
		return nil, fmt.Errorf("expected int16, got %T", data)
	}

	buf = wtintpack.PackInt(buf, int64(v))

	return buf, nil
}

func (p fieldPackerInt16) UnpackField(buf []byte, data any) ([]byte, error) {
	switch v := data.(type) {
	case *int16:
		buf, x := wtintpack.UnpackInt(buf)
		*v = int16(x)
		return buf, nil
	case *any:
		buf, x := wtintpack.UnpackInt(buf)
		*v = int16(x)
		return buf, nil
	default:
		return nil, fmt.Errorf("cannot unpack field into type %T", v)
	}
}

type fieldPackerUint16 struct {
}

func (p fieldPackerUint16) PackField(data any, buf []byte) ([]byte, error) {
	v, ok := data.(uint16)
	if !ok {
		return nil, fmt.Errorf("expected uint16, got %T", data)
	}

	buf = wtintpack.PackUint(buf, uint64(v))

	return buf, nil
}

func (p fieldPackerUint16) UnpackField(buf []byte, data any) ([]byte, error) {
	switch v := data.(type) {
	case *uint16:
		buf, x := wtintpack.UnpackInt(buf)
		*v = uint16(x)
		return buf, nil
	case *any:
		buf, x := wtintpack.UnpackInt(buf)
		*v = uint16(x)
		return buf, nil
	default:
		return nil, fmt.Errorf("cannot unpack field into type %T", v)
	}
}

type fieldPackerInt32 struct {
}

func (p fieldPackerInt32) PackField(data any, buf []byte) ([]byte, error) {
	v, ok := data.(int32)
	if !ok {
		return nil, fmt.Errorf("expected int32, got %T", data)
	}

	buf = wtintpack.PackInt(buf, int64(v))

	return buf, nil
}

func (p fieldPackerInt32) UnpackField(buf []byte, data any) ([]byte, error) {
	switch v := data.(type) {
	case *int32:
		buf, x := wtintpack.UnpackInt(buf)
		*v = int32(x)
		return buf, nil
	case *any:
		buf, x := wtintpack.UnpackInt(buf)
		*v = int32(x)
		return buf, nil
	default:
		return nil, fmt.Errorf("cannot unpack field into type %T", v)
	}
}

type fieldPackerUint32 struct {
}

func (p fieldPackerUint32) PackField(data any, buf []byte) ([]byte, error) {
	v, ok := data.(uint32)
	if !ok {
		return nil, fmt.Errorf("expected uint32, got %T", data)
	}

	buf = wtintpack.PackUint(buf, uint64(v))

	return buf, nil
}

func (p fieldPackerUint32) UnpackField(buf []byte, data any) ([]byte, error) {
	switch v := data.(type) {
	case *uint32:
		buf, x := wtintpack.UnpackInt(buf)
		*v = uint32(x)
		return buf, nil
	case *any:
		buf, x := wtintpack.UnpackInt(buf)
		*v = uint32(x)
		return buf, nil
	default:
		return nil, fmt.Errorf("cannot unpack field into type %T", v)
	}
}

type fieldPackerInt64 struct {
}

func (p fieldPackerInt64) PackField(data any, buf []byte) ([]byte, error) {
	v, ok := data.(int64)
	if !ok {
		return nil, fmt.Errorf("expected int64, got %T", data)
	}

	buf = wtintpack.PackInt(buf, int64(v))

	return buf, nil
}

func (p fieldPackerInt64) UnpackField(buf []byte, data any) ([]byte, error) {
	switch v := data.(type) {
	case *int64:
		buf, x := wtintpack.UnpackInt(buf)
		*v = int64(x)
		return buf, nil
	case *any:
		buf, x := wtintpack.UnpackInt(buf)
		*v = int64(x)
		return buf, nil
	default:
		return nil, fmt.Errorf("cannot unpack field into type %T", v)
	}
}

type fieldPackerUint64 struct {
}

func (p fieldPackerUint64) PackField(data any, buf []byte) ([]byte, error) {
	v, ok := data.(uint64)
	if !ok {
		return nil, fmt.Errorf("expected uint64, got %T", data)
	}

	buf = wtintpack.PackUint(buf, uint64(v))

	return buf, nil
}

func (p fieldPackerUint64) UnpackField(buf []byte, data any) ([]byte, error) {
	switch v := data.(type) {
	case *uint64:
		buf, x := wtintpack.UnpackInt(buf)
		*v = uint64(x)
		return buf, nil
	case *any:
		buf, x := wtintpack.UnpackInt(buf)
		*v = uint64(x)
		return buf, nil
	default:
		return nil, fmt.Errorf("cannot unpack field into type %T", v)
	}
}

type fieldPackerFixedSizeString struct {
	size int
}

func (p fieldPackerFixedSizeString) PackField(data any, buf []byte) ([]byte, error) {
	v, ok := data.(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", data)
	}

	switch {
	case p.size == len(v):
		buf = append(buf, v...)
	case p.size > len(v):
		n := p.size - len(v)
		buf = append(buf, v...)

		for i := 0; i < n; i++ {
			buf = append(buf, byte(0))
		}
	case p.size < len(v):
		buf = append(buf, v[:p.size]...)
	}

	return buf, nil
}
func (p fieldPackerFixedSizeString) UnpackField(buf []byte, data any) ([]byte, error) {
	switch v := data.(type) {
	case *string:
		*v = string(buf[:p.size])
		buf = buf[p.size:]
		return buf, nil
	case *any:
		*v = string(buf[:p.size])
		buf = buf[p.size:]
		return buf, nil
	default:
		return nil, fmt.Errorf("cannot unpack field into type %T", v)
	}
}

type fieldPackerNullTerminatedString struct {
	size int
}

func (p fieldPackerNullTerminatedString) UnpackField(buf []byte, data any) ([]byte, error) {
	var s string

	if p.size > 0 {
		s = string(buf[:p.size])
		buf = buf[p.size:]
		if len(buf) > 0 && buf[0] == 0 {
			buf = buf[1:]
		}
	} else {
		n := bytes.IndexByte(buf, 0)
		switch {
		case n == 0:
			s = ""
			buf = buf[1:]
		case n > 0:
			s = string(buf[:n])
			buf = buf[n+1:]
		default:
			return nil, fmt.Errorf("malformed field")
		}
	}

	switch v := data.(type) {
	case *string:
		*v = s
		return buf, nil
	case *any:
		*v = s
		return buf, nil
	default:
		return nil, fmt.Errorf("cannot unpack field into type %T", v)
	}
}

func (p fieldPackerNullTerminatedString) PackField(data any, buf []byte) ([]byte, error) {
	v, ok := data.(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", data)
	}

	if p.size > 0 {
		switch {
		case len(v) > p.size:
			buf = append(buf, v...)
		case len(v) < p.size:
			buf = append(buf, v...)
			for i := 0; i < p.size-len(v); i++ {
				buf = append(buf, byte(0))
			}
		default:
			n := strings.LastIndexByte(v, 0)
			if n == -1 {
				buf = append(buf, v...)
				buf = append(buf, byte(0))
			} else {
				buf = append(buf, v[:n+1]...)
			}
		}
	} else {
		n := strings.LastIndexByte(v, 0)
		if n == -1 {
			buf = append(buf, v...)
			buf = append(buf, byte(0))
		} else {
			buf = append(buf, v[:n+1]...)
		}
	}

	return buf, nil
}

func ParseFormat(format string) ([]FieldPacker, error) {
	packers := make([]FieldPacker, 0, 4)

	var size int
	var parsingSize bool

	for i := 0; i < len(format); i++ {
		char := format[i]

		if char >= '0' && char <= '9' {
			size = size*10 + int(char-'0')
			if !parsingSize {
				parsingSize = true
			}

			continue
		}

		switch char {
		case 'b':
			if size == 0 {
				size = 1
			}

			for i := 0; i < size; i++ {
				packers = append(packers, fieldPackerInt8{})
			}
		case 'B':
			if size == 0 {
				size = 1
			}

			for i := 0; i < size; i++ {
				packers = append(packers, fieldPackerUint8{})
			}
		case 'h':
			if size == 0 {
				size = 1
			}

			for i := 0; i < size; i++ {
				packers = append(packers, fieldPackerInt16{})
			}
		case 'H':
			if size == 0 {
				size = 1
			}

			for i := 0; i < size; i++ {
				packers = append(packers, fieldPackerUint16{})
			}
		case 'i', 'l':
			if size == 0 {
				size = 1
			}

			for i := 0; i < size; i++ {
				packers = append(packers, fieldPackerInt32{})
			}
		case 'I', 'L':
			if size == 0 {
				size = 1
			}

			for i := 0; i < size; i++ {
				packers = append(packers, fieldPackerUint32{})
			}
		case 'q':
			if size == 0 {
				size = 1
			}

			for i := 0; i < size; i++ {
				packers = append(packers, fieldPackerInt64{})
			}
		case 'Q', 'r':
			if size == 0 {
				size = 1
			}

			for i := 0; i < size; i++ {
				packers = append(packers, fieldPackerUint64{})
			}
		case 's':
			s := size
			if s == 0 {
				s = 1
			}

			packers = append(packers, fieldPackerFixedSizeString{size: s})
		case 'S':
			packers = append(packers, fieldPackerNullTerminatedString{size: size})
		default:
			return nil, fmt.Errorf("'%s' is not a supported format directive", string(char))
		}

		if parsingSize {
			size = 0
			parsingSize = false
		}
	}

	return packers, nil
}
