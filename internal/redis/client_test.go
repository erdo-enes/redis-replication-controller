package redis

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"reflect"
	"testing"
	"time"
)

// fakeServer starts a one-connection TCP server that reads RESP commands and
// replies with whatever respond returns. Returning nil closes the connection.
func fakeServer(t *testing.T, respond func(cmd []string) []byte) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		br := bufio.NewReader(conn)
		for {
			cmd, err := readServerCommand(br)
			if err != nil {
				return
			}
			reply := respond(cmd)
			if reply == nil {
				return
			}
			if _, err := conn.Write(reply); err != nil {
				return
			}
		}
	}()
	return ln.Addr().String()
}

func readServerCommand(r *bufio.Reader) ([]string, error) {
	reply, err := readReply(r)
	if err != nil {
		return nil, err
	}
	arr, ok := reply.([]interface{})
	if !ok {
		return nil, fmt.Errorf("expected array command, got %T", reply)
	}
	out := make([]string, len(arr))
	for i, e := range arr {
		out[i], _ = e.(string)
	}
	return out, nil
}

func capturingServer(t *testing.T, reply []byte) (string, chan []string) {
	got := make(chan []string, 8)
	addr := fakeServer(t, func(cmd []string) []byte {
		got <- cmd
		return reply
	})
	return addr, got
}

func dialTest(t *testing.T, addr string, cmdTimeout time.Duration) Conn {
	t.Helper()
	conn, err := NewDialer(time.Second, cmdTimeout).Dial(context.Background(), addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func TestPingSuccess(t *testing.T) {
	addr, got := capturingServer(t, []byte("+PONG\r\n"))
	conn := dialTest(t, addr, time.Second)
	if err := conn.Ping(context.Background()); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	if cmd := <-got; len(cmd) != 1 || cmd[0] != "PING" {
		t.Fatalf("server received %v", cmd)
	}
}

func TestPingServerError(t *testing.T) {
	addr := fakeServer(t, func([]string) []byte { return []byte("-LOADING Redis is loading the dataset in memory\r\n") })
	conn := dialTest(t, addr, time.Second)
	if err := conn.Ping(context.Background()); err == nil {
		t.Fatal("expected error from LOADING reply")
	}
}

func TestPingConnectionRefused(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close() // nothing is listening on addr anymore

	_, err = NewDialer(300*time.Millisecond, time.Second).Dial(context.Background(), addr)
	if err == nil {
		t.Fatal("expected connection refused error")
	}
}

func TestCommandTimeout(t *testing.T) {
	addr := fakeServer(t, func([]string) []byte {
		time.Sleep(300 * time.Millisecond)
		return []byte("+PONG\r\n")
	})
	conn := dialTest(t, addr, 50*time.Millisecond)
	if err := conn.Ping(context.Background()); err == nil {
		t.Fatal("expected timeout error")
	}
}

func TestReplicaOfSendsCorrectCommand(t *testing.T) {
	addr, got := capturingServer(t, []byte("+OK\r\n"))
	conn := dialTest(t, addr, time.Second)
	if err := conn.ReplicaOf(context.Background(), "10.0.0.1", 6379); err != nil {
		t.Fatal(err)
	}
	want := []string{"REPLICAOF", "10.0.0.1", "6379"}
	if cmd := <-got; !reflect.DeepEqual(cmd, want) {
		t.Fatalf("got %v, want %v", cmd, want)
	}
}

func TestReplicaOfNoOneSendsCorrectCommand(t *testing.T) {
	addr, got := capturingServer(t, []byte("+OK\r\n"))
	conn := dialTest(t, addr, time.Second)
	if err := conn.ReplicaOfNoOne(context.Background()); err != nil {
		t.Fatal(err)
	}
	want := []string{"REPLICAOF", "NO", "ONE"}
	if cmd := <-got; !reflect.DeepEqual(cmd, want) {
		t.Fatalf("got %v, want %v", cmd, want)
	}
}

func TestRoleMaster(t *testing.T) {
	addr := fakeServer(t, func([]string) []byte { return []byte("*3\r\n$6\r\nmaster\r\n:100\r\n*0\r\n") })
	conn := dialTest(t, addr, time.Second)
	info, err := conn.Role(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if info.Role != RoleMaster || info.ReplOffset != 100 {
		t.Fatalf("got %+v", info)
	}
}

func TestInfoReplicationSlave(t *testing.T) {
	body := "# Replication\r\nrole:slave\r\nmaster_host:1.2.3.4\r\nmaster_port:6379\r\nmaster_link_status:up\r\nslave_repl_offset:5\r\n"
	bulk := fmt.Sprintf("$%d\r\n%s\r\n", len(body), body)
	addr := fakeServer(t, func([]string) []byte { return []byte(bulk) })
	conn := dialTest(t, addr, time.Second)
	info, err := conn.InfoReplication(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !info.IsReplica() || info.MasterHost != "1.2.3.4" || info.Offset() != 5 {
		t.Fatalf("got %+v", info)
	}
}
