package api

import (
	"fmt"

	"github.com/pkg/errors"
)

type Error interface {
	Error() string
	Code() int
	Message() string
	Cause() error
}

type baseError struct {
	Err  error
	Msg  string
	Code int
}

func (x *baseError) Message() string {
	if x.Msg != "" {
		return x.Msg
	}

	return x.Err.Error()
}

func (x *baseError) Cause() error {
	return x.Err
}

type userError struct{ baseError }

func (x *userError) Error() string { return "UserError: " + x.Msg + "\n" + x.Err.Error() }
func (x *userError) Code() int {
	if x.baseError.Code > 0 {
		return x.baseError.Code
	}
	return 400
}

func wrapUserError(err error, code int, msg string) Error {
	return &userError{
		baseError: baseError{
			Err:  errors.Wrap(err, msg),
			Code: code,
		},
	}
}

func newUserErrorf(code int, msg string, args ...interface{}) Error {
	return &userError{
		baseError: baseError{
			Msg:  fmt.Sprintf(msg, args...),
			Code: code,
		},
	}
}

type systemError struct{ baseError }

func (x *systemError) Error() string { return "SystemError: " + x.Msg + "\n" + x.Err.Error() }
func (x *systemError) Code() int {
	if x.baseError.Code > 0 {
		return x.baseError.Code
	}
	return 500
}

func wrapSystemError(err error, code int, msg string) Error {
	return &systemError{
		baseError: baseError{
			Err:  errors.Wrap(err, msg),
			Code: code,
		},
	}
}

func wrapSystemErrorf(err error, code int, msg string, args ...interface{}) Error {
	return &systemError{
		baseError: baseError{
			Err:  errors.Wrap(err, fmt.Sprintf(msg, args...)),
			Code: code,
		},
	}
}

func newSystemError(msg string, code int) Error {
	return &systemError{
		baseError: baseError{
			Msg:  msg,
			Code: code,
		},
	}
}
