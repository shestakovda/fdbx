package fdbx_test

import (
	"errors"
	"testing"

	"github.com/shestakovda/fdbx"
	"github.com/stretchr/testify/assert"
)

func TestConn(t *testing.T) {
	c1, err := fdbx.NewConn(0, fdbx.ConnVersion610)
	assert.NoError(t, err)
	assert.NotNil(t, c1)

	// ************ Key ************

	_, err = c1.Key(nil)
	assert.True(t, errors.Is(err, fdbx.ErrNullModel))
}
