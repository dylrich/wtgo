package wtintpack

import (
	"math"
)

const (
	negMultiMarker byte = 0x10
	neg2ByteMarker byte = 0x20
	neg1ByteMarker byte = 0x40
	pos1ByteMarker byte = 0x80
	pos2ByteMarker byte = 0xc0
	posMultiMarker byte = 0xe0

	neg1ByteMin int64 = (-(1 << 6))                // -64
	neg2ByteMin int64 = (-(1 << 13) + neg1ByteMin) // -8256
	pos1ByteMax int64 = ((1 << 6) - 1)             // 63
	pos2ByteMax int64 = ((1 << 13) + pos1ByteMax)  // 8255
)

func getBitsU64(x uint64, start, end uint) uint64 {
	const u1 uint64 = 1
	return (x & ((u1 << start) - u1)) >> end
}

func getBitsI64(x int64, start, end uint) int64 {
	const u1 int64 = 1
	return (x & ((u1 << start) - u1)) >> end
}

func wtLeadingZeros(x uint64, i byte) byte {
	m := uint64(0xFF << 56)

	for i = 0; (x&m) == 0 && i != 8; i++ {
		m >>= 8
	}

	return i
}

func vpackPosInt(buf []byte, x uint64) []byte {
	var lz byte

	lz = wtLeadingZeros(x, lz)
	length := 8 - lz

	prefix := byte(posMultiMarker | (length & 0xf))
	buf = append(buf, prefix)

	for shift := (length - 1) << 3; length != 0; length-- {
		buf = append(buf, byte(x>>shift))
		shift -= 8
	}

	return buf
}

func PackUint(buf []byte, x uint64) []byte {
	switch {
	case x <= uint64(pos1ByteMax):
		buf = append(buf, pos1ByteMarker|byte(getBitsU64(x, 6, 0)))
	case x <= uint64(pos2ByteMax):
		x -= uint64(pos1ByteMax) + 1
		buf = append(buf, pos2ByteMarker|byte(getBitsU64(x, 13, 8)))
		buf = append(buf, byte(getBitsU64(x, 8, 0)))
	case x == uint64(pos2ByteMax+1):
		buf = append(buf, byte(posMultiMarker|0x01))
		buf = append(buf, byte(0))
	default:
		x -= uint64(pos2ByteMax) + 1
		buf = vpackPosInt(buf, x)
	}

	return buf
}

func vpackNegInt(buf []byte, x uint64) []byte {
	var lz byte

	lz = wtLeadingZeros(^x, lz)
	length := 8 - lz

	prefix := byte(negMultiMarker | (lz & 0xf))
	buf = append(buf, prefix)

	for shift := (length - 1) << 3; length != 0; length-- {
		buf = append(buf, byte(x>>shift))
		shift -= 8
	}

	return buf
}

func PackInt(buf []byte, x int64) []byte {
	switch {
	case x < neg2ByteMin:
		buf = vpackNegInt(buf, uint64(x))
	case x < neg1ByteMin:
		x -= neg2ByteMin
		buf = append(buf, neg2ByteMarker|byte(getBitsI64(x, 13, 8)))
		buf = append(buf, byte(getBitsI64(x, 8, 0)))
	case x < 0:
		x -= neg1ByteMin
		buf = append(buf, neg1ByteMarker|byte(getBitsU64(uint64(x), 6, 0)))
	default:
		buf = PackUint(buf, uint64(x))
	}

	return buf
}

func UnpackUint(buf []byte) ([]byte, uint64) {
	switch buf[0] & 0xf0 {
	case pos1ByteMarker, pos1ByteMarker | 0x10, pos1ByteMarker | 0x20, pos1ByteMarker | 0x30:
		x := getBitsU64(uint64(buf[0]), 6, 0)
		buf = buf[1:]

		return buf, x

	case pos2ByteMarker, pos2ByteMarker | 0x10:
		x := getBitsU64(uint64(buf[0]), 5, 0) << 8
		x |= uint64(buf[1])
		x += uint64(pos1ByteMax + 1)
		buf = buf[2:]

		return buf, x

	case posMultiMarker:
		buf, x := vUnpackPosInt(buf)
		n := uint64(pos2ByteMax + 1)
		x += n

		return buf, x
	}

	return buf, 0
}

func vUnpackPosInt(buf []byte) ([]byte, uint64) {
	length := buf[0] & 0xf

	buf = buf[1:]

	var x uint64

	for ; length != 0; length-- {
		x = (x << 8) | uint64(buf[0])
		buf = buf[1:]
	}

	return buf, x
}

func vUnpackNegInt(buf []byte) ([]byte, int64) {
	length := int64(8 - (buf[0] & 0xf))

	buf = buf[1:]

	// TODO: bounds check?

	x := uint64(math.MaxUint64)

	for ; length != 0; length-- {
		x = (x << 8) | uint64(buf[0])
		buf = buf[1:]
	}

	return buf, int64(x)
}

func UnpackInt(buf []byte) ([]byte, int64) {
	switch buf[0] & 0xF0 {
	case negMultiMarker:
		buf, x := vUnpackNegInt(buf)

		return buf, x
	case neg2ByteMarker, neg2ByteMarker | 0x10:
		// TODO: bounds check?
		x := int64(getBitsU64(uint64(buf[0]), 5, 0) << 8)
		x |= int64(buf[1])
		x += neg2ByteMin

		return buf[2:], x
	case neg1ByteMarker, neg1ByteMarker | 0x10, neg1ByteMarker | 0x20, neg1ByteMarker | 0x30:
		x := neg1ByteMin + int64(getBitsU64(uint64(buf[0]), 6, 0))

		return buf[1:], x
	default:
		b, x := UnpackUint(buf)

		return b, int64(x)
	}
}
