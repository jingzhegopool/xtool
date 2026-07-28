package pool

import (
	"crypto/rand"
	"fmt"
)

// newID generates a UUID v4 using crypto/rand.
// Zero external dependencies, pure standard library.
func newID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	// UUID v4: set version bits (4) in byte 6
	b[6] = (b[6] & 0x0f) | 0x40
	// Set variant bits (10) in byte 8
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
