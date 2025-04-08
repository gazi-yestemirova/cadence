// Copyright (c) 2017 Uber Technologies, Inc.
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
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package clusterredirection

import (
	"context"

	"github.com/stretchr/testify/mock"

	"github.com/uber/cadence/common/types"
)

// MockClusterRedirectionPolicy is an autogenerated mock type for the ClusterRedirectionPolicy type
type MockClusterRedirectionPolicy struct {
	mock.Mock
}

// WithDomainIDRedirect provides a mock function with given fields: domainID, apiName, call
func (_m *MockClusterRedirectionPolicy) WithDomainIDRedirect(ctx context.Context, domainID string, apiName string, requestedConsistencyLevel types.QueryConsistencyLevel, call func(string) error) error {
	ret := _m.Called(domainID, apiName, requestedConsistencyLevel, call)

	var r0 error
	if rf, ok := ret.Get(0).(func(string, string, types.QueryConsistencyLevel, func(string) error) error); ok {
		r0 = rf(domainID, apiName, requestedConsistencyLevel, call)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// WithDomainNameRedirect provides a mock function with given fields: domainName, apiName, call
func (_m *MockClusterRedirectionPolicy) WithDomainNameRedirect(ctx context.Context, domainName string, apiName string, requestedConsistencyLevel types.QueryConsistencyLevel, call func(string) error) error {
	ret := _m.Called(domainName, apiName, requestedConsistencyLevel, call)

	var r0 error
	if rf, ok := ret.Get(0).(func(string, string, types.QueryConsistencyLevel, func(string) error) error); ok {
		r0 = rf(domainName, apiName, requestedConsistencyLevel, call)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}
