package search

import "errors"

func IsBadRequest(err error) bool {
	return errors.Is(err, ErrBadRequest)
}
