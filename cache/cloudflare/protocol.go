package cloudflare

import "encoding/binary"

// HTTP batch contract shared by the Go client, the in-memory mock, and the
// Durable Object Worker.
//
// Every request is POSTed to:
//
//	{baseURL}/v1/{tenant}/{op}
//
// where op is one of the constants below. Requests and responses are JSON;
// binary payloads ([]byte fields) are base64-encoded by encoding/json.
//
// A stored value ("blob") is the same wire layout the other dsorm cache
// backends use: a 4-byte little-endian uint32 flags header followed by the
// raw value bytes. The Durable Object treats blobs as opaque except for the
// increment op, which interprets the value as an 8-byte little-endian int64.
const (
	opGet       = "get"
	opSet       = "set"
	opAdd       = "add"
	opCAS       = "cas"
	opDelete    = "delete"
	opIncrement = "incr"
	opFlush     = "flush"
)

// Per-item result codes returned by add/set/cas.
const (
	codeOK        = ""          // stored / no error
	codeNotStored = "notstored" // add conflict, or cas on a missing key
	codeConflict  = "conflict"  // cas value changed since read
)

// wireItem is a single key/value entry in a request or response.
type wireItem struct {
	Key   string `json:"k"`
	Blob  []byte `json:"b,omitempty"` // [4-byte LE flags][value]
	CAS   []byte `json:"c,omitempty"` // original blob, cas op only
	TTLMs int64  `json:"t,omitempty"` // expiry in ms from now; 0 = no expiry
}

// batchRequest is the body for every op. Only the relevant fields are set.
type batchRequest struct {
	Items []wireItem `json:"items,omitempty"` // set/add/cas
	Keys  []string   `json:"keys,omitempty"`  // get/delete
	Key   string     `json:"key,omitempty"`   // incr
	Delta int64      `json:"delta,omitempty"` // incr
	TTLMs int64      `json:"ttl,omitempty"`   // incr
}

// batchResponse is the body returned for every op.
type batchResponse struct {
	Items []wireItem `json:"items,omitempty"` // get: found entries (misses omitted)
	Codes []string   `json:"codes,omitempty"` // add/set/cas: aligned to request Items
	Count int        `json:"count,omitempty"` // delete: live rows removed
	Value int64      `json:"value,omitempty"` // incr: new counter value
}

// encodeBlob packs flags and value into the on-wire/on-storage blob layout.
func encodeBlob(flags uint32, value []byte) []byte {
	b := make([]byte, 4+len(value))
	binary.LittleEndian.PutUint32(b[:4], flags)
	copy(b[4:], value)
	return b
}

// decodeBlob splits a blob into its flags header and value. ok is false when
// the blob is too short to contain the header.
func decodeBlob(blob []byte) (flags uint32, value []byte, ok bool) {
	if len(blob) < 4 {
		return 0, nil, false
	}
	return binary.LittleEndian.Uint32(blob[:4]), blob[4:], true
}
