package wtformat

import (
	"bytes"
	"fmt"
	"strings"
)

type packer struct {
	fields []FieldPacker
}

type FieldPacker interface {
	PackField(data any, buf []byte) ([]byte, error)
	UnpackField(buf []byte, data any) ([]byte, error)
}

type fieldPackerNullTerminatedString struct {
	size int
}

func (p fieldPackerNullTerminatedString) UnpackField(buf []byte, data any) ([]byte, error) {
	v, ok := data.(*string)
	if ok == false {
		return nil, fmt.Errorf("expected string pointer, got %T", data)
	}

	if p.size > 0 {
		*v = string(buf[:p.size])
		buf = buf[p.size:]
	} else {
		n := bytes.IndexByte(buf, 0)
		switch {
		case n == 0:
			*v = ""
			buf = buf[1:]
		case n > 0:
			*v = string(buf[:n])
			buf = buf[n+1:]
		default:
			return nil, fmt.Errorf("malformed field")
		}
	}

	return buf, nil
}

func (p fieldPackerNullTerminatedString) PackField(data any, buf []byte) ([]byte, error) {
	v, ok := data.(string)
	if !ok {
		return nil, fmt.Errorf("not a string")
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
			n := strings.IndexByte(v, 0)
			if n == -1 {
				buf = append(buf, v...)
				buf = append(buf, byte(0))
			} else {
				buf = append(buf, v[:n+1]...)
			}
		}
	} else {
		n := strings.IndexByte(v, 0)
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
