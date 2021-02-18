// Copyright (c) 2020, pole-group. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pole_rpc

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func init()  {
	RpcLog = NewTestLogger("test")
}

var (
	TestHelloWorld = "TestHelloWorld"
	TestPort       = int32(8888)
)

func createTestEndpoint() Endpoint {
	return Endpoint{
		Key:  "",
		Host: "127.0.0.1",
		Port: TestPort,
	}
}

func createTestClient() (TransportClient, error) {
	return NewTransportClient(func(opt *ClientOption) {
		opt.ConnectType = ConnectTypeRSocket
		opt.OpenTSL = false
	})
}

func createTestServer(ctx context.Context) *RSocketServer {
	return newRSocketServer(ctx, ServerOption{
		ConnectType: ConnectTypeRSocket,
		Label:       "pole_rpc_test",
		Port:        TestPort,
		OpenTSL:     false,
	})
}

//TestRSocketClient_Request 测试 request-response 模型是否正常
func TestRSocketClient_Request(t *testing.T) {
	ctx, cancelF := context.WithCancel(context.Background())
	defer cancelF()

	TestPort = 8000 + rand.Int31n(1000)
	server := createTestServer(ctx)
	// 等待 Server 的启动
	<-server.IsReady

	if err := <-server.ErrChan; err != nil {
		t.Error(err)
	}

	client, err := createTestClient()
	if err != nil {
		t.Error(err)
		return
	}

	serverReceive := atomic.Value{}

	waitG := sync.WaitGroup{}
	waitG.Add(1)

	server.RegisterRequestHandler(TestHelloWorld, func(cxt context.Context, rpcCtx RpcServerContext) {
		defer waitG.Done()
		req := rpcCtx.GetReq()
		serverReceive.Store(req)
		_ = rpcCtx.Send(&ServerResponse{
			FunName:   TestHelloWorld,
			RequestId: req.RequestId,
		})
	})

	reqId := uuid.New().String()

	cResp, err := client.Request(context.Background(), createTestEndpoint(), &ServerRequest{
		FunName:   TestHelloWorld,
		RequestId: reqId,
	})

	if err != nil {
		t.Error(err)
		return
	}

	waitG.Wait()

	sReq := serverReceive.Load().(*ServerRequest)

	assert.Equalf(t, reqId, sReq.RequestId, "server receive request-id must be equals client request-id")
	assert.Equalf(t, reqId, cResp.RequestId, "client receive response-id must be equals client request-id")
}

//Test_RequestId_ServerCtx_Change 测试 channel 模型是否工作正常
func Test_RequestId_ServerCtx_Change(t *testing.T) {
	ctx, cancelF := context.WithCancel(context.Background())
	defer cancelF()

	TestPort = 8000 + rand.Int31n(1000)
	server := createTestServer(ctx)
	// 等待 Server 的启动
	<-server.IsReady

	if err := <-server.ErrChan; err != nil {
		t.Error(err)
		return
	}

	client, err := createTestClient()
	if err != nil {
		t.Error(err)
		return
	}
	server.RegisterChannelRequestHandler(TestHelloWorld, func(cxt context.Context, rpcCtx RpcServerContext) {
		fmt.Printf("receive client requst : %#v\n", rpcCtx.GetReq())
		for i := 0; i < 10; i++ {
			_ = rpcCtx.Send(&ServerResponse{
				Code: int32(i),
			})
		}
	})
	waitG := sync.WaitGroup{}
	waitG.Add(10)

	uuidHolder := atomic.Value{}

	call := func(resp *ServerResponse, err error) {
		waitG.Done()
		RpcLog.Info("response %#v", resp)
		assert.Equalf(t, uuidHolder.Load().(string), resp.RequestId, "req-id must equal")
	}

	rpcCtx, err := client.RequestChannel(ctx, createTestEndpoint(), call)
	if err != nil {
		t.Error(err)
		return
	}

	reqId := uuid.New().String()
	uuidHolder.Store(reqId)
	rpcCtx.Send(&ServerRequest{
		FunName:   TestHelloWorld,
		RequestId: reqId,
	})
	waitG.Wait()

	waitG.Add(10)
	reqId = uuid.New().String()
	uuidHolder.Store(reqId)
	rpcCtx.Send(&ServerRequest{
		FunName:   TestHelloWorld,
		RequestId: reqId,
	})
	waitG.Wait()
}

//Test_ClientConnectedEvent 测试链接的 Connected 以及 DisConnected 事件是否正常触发
func Test_ClientConnectedEvent(t *testing.T) {
	ctx, cancelF := context.WithCancel(context.Background())
	defer cancelF()

	TestPort = 8000 + rand.Int31n(1000)
	server := createTestServer(ctx)
	// 等待 Server 的启动
	<-server.IsReady

	if err := <-server.ErrChan; err != nil {
		t.Error(err)
		return
	}

	client, err := createTestClient()
	if err != nil {
		t.Error(err)
		return
	}

	timeout := time.After(time.Duration(3) * time.Second)
	eventHolder := atomic.Value{}

	client.RegisterConnectEventWatcher(func(eventType ConnectEventType, conn net.Conn) {
		eventHolder.Store(eventType)
	})

	_, _ = client.Request(ctx, createTestEndpoint(), &ServerRequest{RequestId: uuid.New().String()})

	<-timeout

	assert.Equalf(t, ConnectEventForConnected, eventHolder.Load(), "must be receive connected event")

	timeout = time.After(time.Duration(3) * time.Second)
	assert.Nil(t, client.Close(), "close must success")

	<-timeout

	assert.Equalf(t, ConnectEventForDisConnected, eventHolder.Load(), "must be receive disconnected event")
}

//Test_ServerConnectedEvent 测试链接的 Connected 以及 DisConnected 事件是否正常触发
func Test_ServerConnectedEvent(t *testing.T) {
	ctx, cancelF := context.WithCancel(context.Background())
	defer cancelF()

	TestPort = 8000 + rand.Int31n(1000)
	server := createTestServer(ctx)
	// 等待 Server 的启动
	<-server.IsReady

	if err := <-server.ErrChan; err != nil {
		t.Error(err)
		return
	}

	client, err := createTestClient()
	if err != nil {
		t.Error(err)
		return
	}

	receiveSignal := make(chan ConnectEventType, 1)

	server.AddConnectEventListener(func(eventType ConnectEventType, con net.Conn) {
		RpcLog.Info("receive connect event : %s", eventType.String())
		receiveSignal <- eventType
	})

	go func() {
		time.AfterFunc(time.Duration(15) * time.Second , func() {
			t.Error("timeout!")
			t.FailNow()
		})
	}()

	_, _ = client.Request(ctx, createTestEndpoint(), &ServerRequest{RequestId: uuid.New().String()})

	assert.Equalf(t, ConnectEventForConnected, <-receiveSignal, "must be receive connected event")

	assert.Nil(t, client.Close(), "close must success")

	assert.Equalf(t, ConnectEventForDisConnected, <-receiveSignal, "must be receive disconnected event")
}
