// Code generated by MockGen. DO NOT EDIT.
// Source: authorizer.go
//
// Generated by this command:
//
//	mockgen -package authorization -source authorizer.go -destination authority_mock.go -self_package github.com/uber/cadence/common/authorization
//

// Package authorization is a generated GoMock package.
package authorization

import (
	context "context"
	reflect "reflect"

	gomock "go.uber.org/mock/gomock"
)

// MockAuthorizer is a mock of Authorizer interface.
type MockAuthorizer struct {
	ctrl     *gomock.Controller
	recorder *MockAuthorizerMockRecorder
	isgomock struct{}
}

// MockAuthorizerMockRecorder is the mock recorder for MockAuthorizer.
type MockAuthorizerMockRecorder struct {
	mock *MockAuthorizer
}

// NewMockAuthorizer creates a new mock instance.
func NewMockAuthorizer(ctrl *gomock.Controller) *MockAuthorizer {
	mock := &MockAuthorizer{ctrl: ctrl}
	mock.recorder = &MockAuthorizerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockAuthorizer) EXPECT() *MockAuthorizerMockRecorder {
	return m.recorder
}

// Authorize mocks base method.
func (m *MockAuthorizer) Authorize(ctx context.Context, attributes *Attributes) (Result, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Authorize", ctx, attributes)
	ret0, _ := ret[0].(Result)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Authorize indicates an expected call of Authorize.
func (mr *MockAuthorizerMockRecorder) Authorize(ctx, attributes any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Authorize", reflect.TypeOf((*MockAuthorizer)(nil).Authorize), ctx, attributes)
}

// MockFilteredRequestBody is a mock of FilteredRequestBody interface.
type MockFilteredRequestBody struct {
	ctrl     *gomock.Controller
	recorder *MockFilteredRequestBodyMockRecorder
	isgomock struct{}
}

// MockFilteredRequestBodyMockRecorder is the mock recorder for MockFilteredRequestBody.
type MockFilteredRequestBodyMockRecorder struct {
	mock *MockFilteredRequestBody
}

// NewMockFilteredRequestBody creates a new mock instance.
func NewMockFilteredRequestBody(ctrl *gomock.Controller) *MockFilteredRequestBody {
	mock := &MockFilteredRequestBody{ctrl: ctrl}
	mock.recorder = &MockFilteredRequestBodyMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockFilteredRequestBody) EXPECT() *MockFilteredRequestBodyMockRecorder {
	return m.recorder
}

// SerializeForLogging mocks base method.
func (m *MockFilteredRequestBody) SerializeForLogging() (string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SerializeForLogging")
	ret0, _ := ret[0].(string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// SerializeForLogging indicates an expected call of SerializeForLogging.
func (mr *MockFilteredRequestBodyMockRecorder) SerializeForLogging() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SerializeForLogging", reflect.TypeOf((*MockFilteredRequestBody)(nil).SerializeForLogging))
}
