// Package payload builds and verifies burn-in message bodies. Per spec §7 the
// CRC32 is computed over the RAW Kafka record value bytes (the connector passes
// the value through unmodified) and the worker-id/sequence/contenthash are
// stamped into Kafka RECORD HEADERS (not into the hashed value), so no JSON
// canonicalization step is needed. Reused unchanged from kubemq-aws/burnin;
// Kafka carries the same instrumentation via headers instead of SQS attributes.
package payload

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math/rand/v2"
	"sort"
	"strconv"
	"strings"
)

// Header/attribute names stamped onto every burn-in publish. These survive the
// connector's Kafka-record-header round-trip unchanged. Hyphens are avoided so
// the same names double as SQS-style attribute keys, so use camelCase.
const (
	AttrWorkerID    = "WorkerId"
	AttrSequence    = "Sequence"
	AttrContentHash = "ContentHash"
	AttrTimestampNS = "TimestampNs"
)

// Build returns a printable body of targetSize bytes (min 1) and its CRC32 hex
// string. The body is random lowercase-hex characters — high-entropy for
// corruption detection but wire-valid. Integrity is verified by re-hashing the
// received body against the contenthash header; the connector passes the value
// through unmodified.
func Build(targetSize int) (body []byte, crcHex string) {
	if targetSize < 1 {
		targetSize = 1
	}
	body = randomHex(targetSize)
	crcHex = fmt.Sprintf("%08x", crc32.ChecksumIEEE(body))
	return body, crcHex
}

// VerifyCRC checks the CRC32 hex tag against the actual body bytes.
func VerifyCRC(body []byte, crcHex string) bool {
	actual := fmt.Sprintf("%08x", crc32.ChecksumIEEE(body))
	return actual == crcHex
}

// CRC32Hex returns the CRC32-IEEE of body as the same 8-hex-digit string that
// Build stamps, so a crafted (non-random) body can carry a ContentHash that
// VerifyCRC accepts.
func CRC32Hex(body []byte) string {
	return fmt.Sprintf("%08x", crc32.ChecksumIEEE(body))
}

// AttrForMD5 is the harness view of a message attribute for canonical MD5.
type AttrForMD5 struct {
	DataType    string // "String" | "Number" | "Binary"
	StringValue string // for String/Number
	BinaryValue []byte // for Binary (raw bytes, NOT base64)
}

// MD5OfBody returns the canonical MD5 of a message body (hex).
func MD5OfBody(body string) string {
	sum := md5.Sum([]byte(body))
	return fmt.Sprintf("%x", sum)
}

// CanonicalMD5OfAttributes reimplements the canonical MD5-of-attributes
// algorithm (attrs sorted by name; per attr length-prefixed name/dataType,
// transport-type byte, then length-prefixed value). Returns hex.
func CanonicalMD5OfAttributes(attrs map[string]AttrForMD5) string {
	names := make([]string, 0, len(attrs))
	for n := range attrs {
		names = append(names, n)
	}
	sort.Strings(names)
	h := md5.New()
	var lenBuf [4]byte
	writeLP := func(b []byte) {
		binary.BigEndian.PutUint32(lenBuf[:], uint32(len(b)))
		h.Write(lenBuf[:])
		h.Write(b)
	}
	for _, n := range names {
		a := attrs[n]
		writeLP([]byte(n))
		writeLP([]byte(a.DataType))
		if strings.HasPrefix(a.DataType, "Binary") {
			h.Write([]byte{2})
			writeLP(a.BinaryValue)
		} else {
			h.Write([]byte{1})
			writeLP([]byte(a.StringValue))
		}
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

// SizeDistribution represents weighted size options for message payloads.
type SizeDistribution struct {
	sizes   []int
	weights []int
	total   int
}

// ParseDistribution parses a "size:weight,size:weight" string.
func ParseDistribution(s string) (*SizeDistribution, error) {
	d := &SizeDistribution{}
	for _, p := range strings.Split(s, ",") {
		kv := strings.SplitN(strings.TrimSpace(p), ":", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("invalid distribution entry: %q", p)
		}
		size, err := strconv.Atoi(strings.TrimSpace(kv[0]))
		if err != nil {
			return nil, fmt.Errorf("invalid size in distribution: %q", kv[0])
		}
		weight, err := strconv.Atoi(strings.TrimSpace(kv[1]))
		if err != nil {
			return nil, fmt.Errorf("invalid weight in distribution: %q", kv[1])
		}
		d.sizes = append(d.sizes, size)
		d.weights = append(d.weights, weight)
		d.total += weight
	}
	if d.total == 0 {
		return nil, fmt.Errorf("distribution total weight must be > 0")
	}
	return d, nil
}

// SelectSize returns a size sampled from the weighted distribution.
func (d *SizeDistribution) SelectSize() int {
	r := rand.IntN(d.total)
	cumulative := 0
	for i, w := range d.weights {
		cumulative += w
		if r < cumulative {
			return d.sizes[i]
		}
	}
	return d.sizes[len(d.sizes)-1]
}

const hexAlphabet = "0123456789abcdef"

// randomHex returns n random lowercase-hex bytes (wire-valid text body).
func randomHex(n int) []byte {
	b := make([]byte, n)
	for i := 0; i < n; {
		v := rand.Uint64()
		for j := 0; j < 16 && i < n; j++ {
			b[i] = hexAlphabet[v&0xf]
			v >>= 4
			i++
		}
	}
	return b
}
