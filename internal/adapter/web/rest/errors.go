package rest

import (
	"errors"
)

var (
	ErrUnsupportedMediaType = errors.New("unsupported Media Type")
	ErrBadRequest           = errors.New("bad request")
)
