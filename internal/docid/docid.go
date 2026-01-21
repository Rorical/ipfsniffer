package docid

import (
	"crypto/sha256"
	"encoding/hex"
)

func ForRootAndPath(rootCID, path string) string {
	h := sha256.Sum256([]byte(rootCID + ":" + path))
	return hex.EncodeToString(h[:])
}
