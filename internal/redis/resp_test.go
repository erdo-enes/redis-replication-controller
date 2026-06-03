package redis

import (
	"bufio"
	"bytes"
	"reflect"
	"strings"
	"testing"
)

func TestWriteCommand(t *testing.T) {
	var buf bytes.Buffer
	if err := writeCommand(&buf, "REPLICAOF", "NO", "ONE"); err != nil {
		t.Fatal(err)
	}
	want := "*3\r\n$9\r\nREPLICAOF\r\n$2\r\nNO\r\n$3\r\nONE\r\n"
	if buf.String() != want {
		t.Fatalf("encoded = %q, want %q", buf.String(), want)
	}
}

func TestReadReply(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want interface{}
	}{
		{"simple string", "+PONG\r\n", "PONG"},
		{"integer", ":12345\r\n", int64(12345)},
		{"bulk string", "$5\r\nhello\r\n", "hello"},
		{"null bulk", "$-1\r\n", nil},
		{"array", "*2\r\n$3\r\nfoo\r\n:7\r\n", []interface{}{"foo", int64(7)}},
		{"nested array (ROLE master)", "*3\r\n$6\r\nmaster\r\n:99\r\n*0\r\n",
			[]interface{}{"master", int64(99), []interface{}{}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := bufio.NewReader(strings.NewReader(tc.in))
			got, err := readReply(r)
			if err != nil {
				t.Fatalf("readReply error: %v", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got %#v, want %#v", got, tc.want)
			}
		})
	}
}

func TestReadReplyError(t *testing.T) {
	r := bufio.NewReader(strings.NewReader("-READONLY You can't write against a read only replica.\r\n"))
	_, err := readReply(r)
	var rerr *RedisError
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errorsAs(err, &rerr) {
		t.Fatalf("error type = %T, want *RedisError", err)
	}
	if !strings.Contains(rerr.Msg, "READONLY") {
		t.Fatalf("error message = %q", rerr.Msg)
	}
}

// errorsAs is a tiny local helper to avoid importing errors just for the test.
func errorsAs(err error, target **RedisError) bool {
	re, ok := err.(*RedisError)
	if ok {
		*target = re
	}
	return ok
}
