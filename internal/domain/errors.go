package domain

import "errors"

var (
	ErrInvalidArgument = errors.New("invalid argument")
	ErrEmptyPackage    = errors.New("package has no numbers")
)
