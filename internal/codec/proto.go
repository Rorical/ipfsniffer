package codec

import (
	"fmt"

	"google.golang.org/protobuf/proto"
)

func Marshal(m proto.Message) ([]byte, error) {
	if m == nil {
		return nil, fmt.Errorf("nil message")
	}
	b, err := proto.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("proto marshal: %w", err)
	}
	return b, nil
}

func Unmarshal(b []byte, m proto.Message) error {
	if m == nil {
		return fmt.Errorf("nil message")
	}
	if len(b) == 0 {
		return fmt.Errorf("empty payload")
	}
	if err := proto.Unmarshal(b, m); err != nil {
		return fmt.Errorf("proto unmarshal: %w", err)
	}
	return nil
}
