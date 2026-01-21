package ipnssniff

import (
	"strings"

	boxoipns "github.com/ipfs/boxo/ipns"
)

func ExtractIPFSPathFromIPNSRecord(recordBytes []byte) (string, bool) {
	rec, err := boxoipns.UnmarshalRecord(recordBytes)
	if err != nil {
		return "", false
	}
	p, err := rec.Value()
	if err != nil {
		return "", false
	}
	s := p.String()
	if strings.HasPrefix(s, "/ipfs/") {
		return s, true
	}
	return "", false
}

func IPNSRoutingKeyToNamePath(key string) (string, bool) {
	// key is expected to be "/ipns/" + binary peerid bytes.
	// It is important we pass raw bytes (not an UTF-8 string of the key path).
	n, err := boxoipns.NameFromRoutingKey([]byte(key))
	if err != nil {
		return "", false
	}
	return "/ipns/" + n.String(), true
}

// IPNSRoutingKeyBytesToNamePath converts the raw key bytes used in the DHT for
// the ipns namespace (peer id bytes) into a stable /ipns/<name> path.
func IPNSRoutingKeyBytesToNamePath(keyBytes []byte) (string, bool) {
	if len(keyBytes) == 0 {
		return "", false
	}
	// NameFromRoutingKey expects the full routing key bytes starting with "/ipns/".
	routingKey := make([]byte, 0, len(keyBytes)+len("/ipns/"))
	routingKey = append(routingKey, []byte("/ipns/")...)
	routingKey = append(routingKey, keyBytes...)

	n, err := boxoipns.NameFromRoutingKey(routingKey)
	if err != nil {
		return "", false
	}
	return "/ipns/" + n.String(), true
}
