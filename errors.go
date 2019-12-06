package fdbx

import (
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
)

const stackTpl = "%s:%d -> %s()"

// All package errors
var (
	ErrMemFail        = newError("Системная ошибка в работе памяти")
	ErrUnknownVersion = newError("Запрос на подключение к неизвестной версии клиента")
	ErrOldVersion     = newError("Подключение к устаревшей версии клиента")
	ErrConnect        = newError("Ошибка подключения к СУБД")
	ErrNullModel      = newError("undefined model")
	ErrNullDB         = newError("undefined db connection")
	ErrEmptyID        = newError("empty identifier")
	ErrEmptyValue     = newError("empty value")
	ErrInvalidGZ      = newError("invalid gzip value")
	ErrQueuePanic     = newError("unknown panic in queue")
	ErrIncompatibleDB = newError("incompatible DB object")
)

func newError(msg string) *fdbxError { return &fdbxError{message: msg} }

type fdbxError struct {
	reason  error
	message string
	stack   []string
}

func (e fdbxError) Error() string {
	text := e.message

	if e.reason != nil {
		text += " -> " + e.reason.Error()
	}

	return text
}

func (e *fdbxError) Unwrap() error { return e.reason }

func (e *fdbxError) WithReason(err error) error {
	e.reason = err
	return e.WithStack()
}

func (e *fdbxError) Is(other error) bool {
	if e.message == other.Error() || e.Error() == other.Error() {
		return true
	}

	if e.reason == nil {
		return false
	}

	return errors.Is(e.reason, other)
}

func (e *fdbxError) WithStack() error {
	e.stack = make([]string, 0, 32)

	i := 1

	pc, file, line, ok := runtime.Caller(i)
	for ok {
		e.stack = append(e.stack, fmt.Sprintf(
			stackTpl,
			filepath.Base(file), line,
			filepath.Base(runtime.FuncForPC(pc).Name()),
		))
		i++
		pc, file, line, ok = runtime.Caller(i)
	}
	return e
}

func (e *fdbxError) Format(f fmt.State, r rune) {
	// На первой строчке - уведомление об ошибке и ее тип, затем свое сообщение
	f.Write([]byte(e.Error()))

	// Если указана причина - она сдвинута правее (рекурсивно)
	if e.reason != nil {
		f.Write([]byte(fmt.Sprintf(" <- %s", e.reason)))
	}

	// Если нужны подробности - пишем стек
	if f.Flag('+') {
		f.Write([]byte("\n\t" + strings.Join(e.stack, "\n\t")))
	}
}
