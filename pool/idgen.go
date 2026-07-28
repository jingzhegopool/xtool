package pool

import (
	"crypto/rand"
	"fmt"
)

// newID 使用 crypto/rand 生成 UUID v4。
func newID() string {
	return newIDStr()
}

// NewID 对外导出的 UUID v4 生成函数，供库使用者手动构造 Task 时使用。
func NewID() string { return newIDStr() }

func newIDStr() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

