package backend

import (
	"context"
	"net/url"

	"go.uber.org/zap"

	"github.com/mercari/grpc-http-proxy"
)

// Client is a dynamic gRPC client that performs reflection
type Client struct {
	logger *zap.Logger
	*clientConn
	*reflectionClient
	*serviceDescriptor
	*methodDescriptor
	InputMessage  *message
	OutputMessage *message
	*stub
	err error
}

// Err returns the error that Client aborted on
func (c *Client) Err() error {
	return c.err
}

// NewClient creates a new client
func NewClient(l *zap.Logger) *Client {
	return &Client{
		logger:            l,
		clientConn:        &clientConn{},
		reflectionClient:  &reflectionClient{},
		serviceDescriptor: &serviceDescriptor{},
		methodDescriptor:  &methodDescriptor{},
		InputMessage:      &message{},
		OutputMessage:     &message{},
		stub:              &stub{},
	}
}

// Connect opens a connection to target
func (c *Client) Connect(ctx context.Context, target *url.URL) {
	if c.err != nil {
		return
	}
	cc, err := newClientConn(ctx, target)
	c.clientConn = cc
	c.err = err
	return
}

// CloseConn closes the underlying connection
func (c *Client) CloseConn() {
	if c.err != nil {
		return
	}
	c.err = c.clientConn.close()
	return
}

func (c *Client) newReflectionClient() {
	if c.err != nil {
		return
	}
	c.reflectionClient = newReflectionClient(c.clientConn)
	return
}

func (c *Client) resolveService(ctx context.Context, serviceName string) {
	c.newReflectionClient()
	if c.err != nil {
		return
	}
	sd, err := c.reflectionClient.resolveService(ctx, serviceName)
	c.err = err
	c.serviceDescriptor = sd
}

func (c *Client) findMethodByName(name string) {
	if c.err != nil {
		return
	}
	md, err := c.serviceDescriptor.findMethodByName(name)
	c.err = err
	c.methodDescriptor = md
	return
}

func (c *Client) loadDescriptors(ctx context.Context, serviceName, methodName string) {
	if c.err != nil {
		return
	}
	c.resolveService(ctx, serviceName)
	c.findMethodByName(methodName)
	c.InputMessage = c.methodDescriptor.getInputType().newMessage()
	c.OutputMessage = c.methodDescriptor.getOutputType().newMessage()
	return
}

func (c *Client) unmarshalInputMessage(b []byte) {
	if c.err != nil {
		return
	}
	err := c.InputMessage.unmarshalJSON(b)
	c.err = err
	return
}

func (c *Client) marshalOutputMessage() proxy.GRPCResponse {
	if c.err != nil {
		return nil
	}
	b, err := c.InputMessage.marshalJSON()
	c.err = err
	return b
}

func (c *Client) newStub() {
	if c.err != nil {
		return
	}
	c.stub = newStub(c.clientConn)
	return
}

func (c *Client) invokeRPC(
	ctx context.Context,
	md *proxy.Metadata) {

	c.newStub()
	if c.err != nil {
		return
	}

	m, err := c.stub.invokeRPC(ctx, c.methodDescriptor, c.InputMessage, md)
	c.err = err
	c.OutputMessage = m
	return
}

// Call performs the gRPC call after doing reflection to obtain type information
func (c *Client) Call(ctx context.Context,
	serviceName, methodName string,
	message []byte,
	md *proxy.Metadata,
) (proxy.GRPCResponse, error) {
	c.loadDescriptors(ctx, serviceName, methodName)
	c.unmarshalInputMessage(message)
	c.invokeRPC(ctx, md)
	response := c.marshalOutputMessage()
	return response, c.err
}
