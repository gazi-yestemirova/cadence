// Copyright (c) 2024 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package domaindeprecation

import (
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"go.uber.org/mock/gomock"

	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/resource"
)

type moduleTestSuite struct {
	suite.Suite
	controller   *gomock.Controller
	mockResource *resource.Test
}

func TestModuleTestSuite(t *testing.T) {
	suite.Run(t, new(moduleTestSuite))
}

func (s *moduleTestSuite) SetupTest() {
	s.controller = gomock.NewController(s.T())
	s.mockResource = resource.NewTest(s.T(), s.controller, metrics.Worker)
}

func (s *moduleTestSuite) TearDownTest() {
	s.mockResource.Finish(s.T())
}

func (s *moduleTestSuite) TestNew() {
	params := Params{
		ServiceClient: s.mockResource.GetSDKClient(),
		ClientBean:    s.mockResource.ClientBean,
		Tally:         tally.NoopScope,
		Logger:        s.mockResource.GetLogger(),
	}

	worker := New(params)
	s.NotNil(worker)
	s.IsType(&domainDeprecator{}, worker)
}

func (s *moduleTestSuite) TestStart() {
	params := Params{
		ServiceClient: s.mockResource.GetSDKClient(),
		ClientBean:    s.mockResource.ClientBean,
		Tally:         tally.NoopScope,
		Logger:        s.mockResource.GetLogger(),
	}

	worker := New(params)
	err := worker.Start()
	s.NoError(err)
}

func (s *moduleTestSuite) TestStop() {
	params := Params{
		ServiceClient: s.mockResource.GetSDKClient(),
		ClientBean:    s.mockResource.ClientBean,
		Tally:         tally.NoopScope,
		Logger:        s.mockResource.GetLogger(),
	}

	worker := New(params)
	worker.Stop()
}
