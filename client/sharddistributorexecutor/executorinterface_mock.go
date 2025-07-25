// Code generated by MockGen. DO NOT EDIT.
// Source: executorinterface.go
//
// Generated by this command:
//
//	mockgen -package sharddistributorexecutor -source executorinterface.go -destination executorinterface_mock.go -self_package github.com/uber/cadence/client/sharddistributorexecutor
//

// Package sharddistributorexecutor is a generated GoMock package.
package sharddistributorexecutor

import (
	context "context"
	reflect "reflect"

	gomock "go.uber.org/mock/gomock"
	yarpc "go.uber.org/yarpc"

	types "github.com/uber/cadence/common/types"
)

// MockClient is a mock of Client interface.
type MockClient struct {
	ctrl     *gomock.Controller
	recorder *MockClientMockRecorder
	isgomock struct{}
}

// MockClientMockRecorder is the mock recorder for MockClient.
type MockClientMockRecorder struct {
	mock *MockClient
}

// NewMockClient creates a new mock instance.
func NewMockClient(ctrl *gomock.Controller) *MockClient {
	mock := &MockClient{ctrl: ctrl}
	mock.recorder = &MockClientMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockClient) EXPECT() *MockClientMockRecorder {
	return m.recorder
}

// Heartbeat mocks base method.
func (m *MockClient) Heartbeat(arg0 context.Context, arg1 *types.ExecutorHeartbeatRequest, arg2 ...yarpc.CallOption) (*types.ExecutorHeartbeatResponse, error) {
	m.ctrl.T.Helper()
	varargs := []any{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "Heartbeat", varargs...)
	ret0, _ := ret[0].(*types.ExecutorHeartbeatResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Heartbeat indicates an expected call of Heartbeat.
func (mr *MockClientMockRecorder) Heartbeat(arg0, arg1 any, arg2 ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Heartbeat", reflect.TypeOf((*MockClient)(nil).Heartbeat), varargs...)
}
