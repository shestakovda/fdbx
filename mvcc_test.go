package fdbx_test

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

func TestMVCC(t *testing.T) {
	suite.Run(t, new(MVCCSuite))
}

type MVCCSuite struct {
	suite.Suite
}

func (s *MVCCSuite) TestWorkflow() {

}
