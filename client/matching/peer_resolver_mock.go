// The MIT License (MIT)

// Copyright (c) 2017-2020 Uber Technologies Inc.

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

// Code generated by MockGen. DO NOT EDIT.
// Source: peer_resolver.go

// Package matching is a generated GoMock package.
package matching

import (
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
)

// MockPeerResolver is a mock of PeerResolver interface.
type MockPeerResolver struct {
	ctrl     *gomock.Controller
	recorder *MockPeerResolverMockRecorder
}

// MockPeerResolverMockRecorder is the mock recorder for MockPeerResolver.
type MockPeerResolverMockRecorder struct {
	mock *MockPeerResolver
}

// NewMockPeerResolver creates a new mock instance.
func NewMockPeerResolver(ctrl *gomock.Controller) *MockPeerResolver {
	mock := &MockPeerResolver{ctrl: ctrl}
	mock.recorder = &MockPeerResolverMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockPeerResolver) EXPECT() *MockPeerResolverMockRecorder {
	return m.recorder
}

// FromHostAddress mocks base method.
func (m *MockPeerResolver) FromHostAddress(hostAddress string) (string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "FromHostAddress", hostAddress)
	ret0, _ := ret[0].(string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// FromHostAddress indicates an expected call of FromHostAddress.
func (mr *MockPeerResolverMockRecorder) FromHostAddress(hostAddress interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "FromHostAddress", reflect.TypeOf((*MockPeerResolver)(nil).FromHostAddress), hostAddress)
}

// FromTaskList mocks base method.
func (m *MockPeerResolver) FromTaskList(taskListName string) (string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "FromTaskList", taskListName)
	ret0, _ := ret[0].(string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// FromTaskList indicates an expected call of FromTaskList.
func (mr *MockPeerResolverMockRecorder) FromTaskList(taskListName interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "FromTaskList", reflect.TypeOf((*MockPeerResolver)(nil).FromTaskList), taskListName)
}

// GetAllPeers mocks base method.
func (m *MockPeerResolver) GetAllPeers() ([]string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetAllPeers")
	ret0, _ := ret[0].([]string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetAllPeers indicates an expected call of GetAllPeers.
func (mr *MockPeerResolverMockRecorder) GetAllPeers() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetAllPeers", reflect.TypeOf((*MockPeerResolver)(nil).GetAllPeers))
}