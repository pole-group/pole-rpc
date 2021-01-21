// Copyright (c) 2020, pole-group. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pole_rpc

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

var (
	TestHelloWorld = "TestHelloWorld"
	TestPort       = int32(8888)
)

func createTestEndpointRepository() *EndpointRepository {
	repository := NewEndpointRepository()

	repository.Put(TestHelloWorld, Endpoint{
		Key:  "",
		Host: "127.0.0.1",
		Port: TestPort,
	})

	return repository
}

func createTestClient() (TransportClient, error) {
	return NewTransportClient(ConnectTypeRSocket, createTestEndpointRepository(), false)
}

func createTestServer(ctx context.Context) *RSocketServer {
	return NewRSocketServer(ctx, "lraft_test", TestPort, false)
}

func TestRSocketClient_Request(t *testing.T) {
	ctx, cancelF := context.WithCancel(context.Background())
	defer cancelF()

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
		rpcCtx.Send(&ServerResponse{
			FunName:     TestHelloWorld,
			RequestId: req.RequestId,
		})
	})

	reqId := uuid.New().String()

	cResp, err := client.Request(context.Background(), TestHelloWorld, &ServerRequest{
		FunName:     TestHelloWorld,
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

func Test_RequestId_ServerCtx_Change(t *testing.T) {
	ctx, cancelF := context.WithCancel(context.Background())
	defer cancelF()

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
		for i := 0; i < 10; i ++ {
			rpcCtx.Send(&ServerResponse{
				FunName:       rpcCtx.GetReq().FunName,
				RequestId:     rpcCtx.GetReq().RequestId,
				Code:          int32(i),
			})
		}
	})

	waitGOne := sync.WaitGroup{}
	waitGOne.Add(10)

	waitGTwo := sync.WaitGroup{}
	waitGTwo.Add(20)

	uuidHolder := atomic.Value{}

	call := func(resp *ServerResponse, err error) {
		defer func() {
			_ = recover()
		}()
		waitGOne.Done()
		waitGTwo.Done()
		fmt.Printf("response %#v\n", resp)
		assert.Equalf(t, uuidHolder.Load().(string), resp.RequestId, "req-id must equal")
	}

	rpcCtx, err := client.RequestChannel(ctx, TestHelloWorld, call)
	if err != nil {
		t.Error(err)
		return
	}

	reqId := uuid.New().String()

	uuidHolder.Store(reqId)

	rpcCtx.Send(&ServerRequest{
		FunName:       TestHelloWorld,
		RequestId:     reqId,
	})

	waitGOne.Wait()

	reqId = uuid.New().String()

	uuidHolder.Store(reqId)

	rpcCtx.Send(&ServerRequest{
		FunName:       TestHelloWorld,
		RequestId:     reqId,
	})

	waitGTwo.Wait()
}
