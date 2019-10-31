package fdbx

import (
	"errors"
)

// All package errors
var (
	ErrMemFail        = newError("Системная ошибка в работе памяти")
	ErrUnknownVersion = newError("Запрос на подключение к неизвестной версии клиента")
	ErrOldVersion     = newError("Подключение к устаревшей версии клиента")
	ErrConnect        = newError("Ошибка подключения к СУБД")
	ErrNullModel      = newError("Чтение неинициализированной модели")
	ErrEmptyModelID   = newError("Чтение модели без идентификатора")
)

func newError(msg string) *fdbxError { return &fdbxError{message: msg} }

type fdbxError struct {
	reason  error
	message string
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
	return e
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
