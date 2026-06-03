package redis

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strconv"
	"time"
)

// Conn is a single live connection to a Redis server exposing only the
// commands the controller uses. It is an interface so the controller can be
// tested with an in-memory fake.
type Conn interface {
	Ping(ctx context.Context) error
	Role(ctx context.Context) (*RoleInfo, error)
	InfoReplication(ctx context.Context) (*ReplicationInfo, error)
	ReplicaOf(ctx context.Context, host string, port int) error
	ReplicaOfNoOne(ctx context.Context) error
	ConfigRewrite(ctx context.Context) error
	Close() error
}

// Dialer opens connections to Redis servers addressed as "host:port".
type Dialer interface {
	Dial(ctx context.Context, addr string) (Conn, error)
}

// TCPDialer is the production Dialer backed by real TCP sockets.
type TCPDialer struct {
	ConnectTimeout time.Duration
	CommandTimeout time.Duration
}

// NewDialer returns a TCPDialer with the supplied timeouts.
func NewDialer(connectTimeout, commandTimeout time.Duration) *TCPDialer {
	return &TCPDialer{ConnectTimeout: connectTimeout, CommandTimeout: commandTimeout}
}

// Dial connects to addr and returns a ready-to-use Conn.
func (d *TCPDialer) Dial(ctx context.Context, addr string) (Conn, error) {
	nd := net.Dialer{Timeout: d.ConnectTimeout}
	c, err := nd.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	return &tcpConn{conn: c, r: bufio.NewReader(c), cmdTimeout: d.CommandTimeout}, nil
}

type tcpConn struct {
	conn       net.Conn
	r          *bufio.Reader
	cmdTimeout time.Duration
}

func (c *tcpConn) Close() error { return c.conn.Close() }

// do writes a command and reads a single reply, applying the command timeout
// (or the context deadline, whichever is sooner) to the whole exchange.
func (c *tcpConn) do(ctx context.Context, args ...string) (interface{}, error) {
	deadline := time.Now().Add(c.cmdTimeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	if err := c.conn.SetDeadline(deadline); err != nil {
		return nil, err
	}
	if err := writeCommand(c.conn, args...); err != nil {
		return nil, err
	}
	return readReply(c.r)
}

func (c *tcpConn) Ping(ctx context.Context) error {
	reply, err := c.do(ctx, "PING")
	if err != nil {
		return err
	}
	if s, ok := reply.(string); ok && s == "PONG" {
		return nil
	}
	return fmt.Errorf("redis: unexpected PING reply %v", reply)
}

func (c *tcpConn) Role(ctx context.Context) (*RoleInfo, error) {
	reply, err := c.do(ctx, "ROLE")
	if err != nil {
		return nil, err
	}
	return parseRoleReply(reply)
}

func (c *tcpConn) InfoReplication(ctx context.Context) (*ReplicationInfo, error) {
	reply, err := c.do(ctx, "INFO", "replication")
	if err != nil {
		return nil, err
	}
	s, ok := reply.(string)
	if !ok {
		return nil, fmt.Errorf("redis: unexpected INFO reply %T", reply)
	}
	return parseReplicationInfo(s), nil
}

func (c *tcpConn) ReplicaOf(ctx context.Context, host string, port int) error {
	_, err := c.do(ctx, "REPLICAOF", host, strconv.Itoa(port))
	return err
}

func (c *tcpConn) ReplicaOfNoOne(ctx context.Context) error {
	_, err := c.do(ctx, "REPLICAOF", "NO", "ONE")
	return err
}

func (c *tcpConn) ConfigRewrite(ctx context.Context) error {
	_, err := c.do(ctx, "CONFIG", "REWRITE")
	return err
}
