// Package redis implements a minimal RESP (REdis Serialization Protocol)
// client containing only the commands the controller needs: PING, ROLE,
// INFO replication, REPLICAOF and CONFIG REWRITE.
//
// It intentionally has no external dependencies so the protocol handling can
// be unit tested in isolation and the controller stays small.
package redis

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
)

// RedisError represents an error reply ("-ERR ...") returned by the server.
type RedisError struct{ Msg string }

func (e *RedisError) Error() string { return e.Msg }

// writeCommand encodes args as a RESP array of bulk strings and writes it to w.
func writeCommand(w io.Writer, args ...string) error {
	if len(args) == 0 {
		return errors.New("redis: empty command")
	}
	var b []byte
	b = append(b, '*')
	b = strconv.AppendInt(b, int64(len(args)), 10)
	b = append(b, '\r', '\n')
	for _, a := range args {
		b = append(b, '$')
		b = strconv.AppendInt(b, int64(len(a)), 10)
		b = append(b, '\r', '\n')
		b = append(b, a...)
		b = append(b, '\r', '\n')
	}
	_, err := w.Write(b)
	return err
}

// readReply decodes a single RESP reply. The returned value is one of:
// string (simple or bulk string), int64 (integer), nil (null), or
// []interface{} (array). Error replies are returned as a *RedisError error.
func readReply(r *bufio.Reader) (interface{}, error) {
	line, err := readLine(r)
	if err != nil {
		return nil, err
	}
	if len(line) == 0 {
		return nil, errors.New("redis: empty reply line")
	}
	prefix, rest := line[0], string(line[1:])
	switch prefix {
	case '+': // simple string
		return rest, nil
	case '-': // error
		return nil, &RedisError{Msg: rest}
	case ':': // integer
		return strconv.ParseInt(rest, 10, 64)
	case '$': // bulk string
		n, err := strconv.Atoi(rest)
		if err != nil {
			return nil, fmt.Errorf("redis: bad bulk length %q: %w", rest, err)
		}
		if n < 0 {
			return nil, nil // null bulk string
		}
		buf := make([]byte, n+2) // include trailing CRLF
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		return string(buf[:n]), nil
	case '*': // array
		n, err := strconv.Atoi(rest)
		if err != nil {
			return nil, fmt.Errorf("redis: bad array length %q: %w", rest, err)
		}
		if n < 0 {
			return nil, nil // null array
		}
		arr := make([]interface{}, n)
		for i := 0; i < n; i++ {
			el, err := readReply(r)
			if err != nil {
				return nil, err
			}
			arr[i] = el
		}
		return arr, nil
	default:
		return nil, fmt.Errorf("redis: unknown reply type %q", prefix)
	}
}

// readLine reads a single CRLF-terminated line and returns it without the
// trailing CR/LF.
func readLine(r *bufio.Reader) ([]byte, error) {
	line, err := r.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	n := len(line)
	if n >= 2 && line[n-2] == '\r' {
		return line[:n-2], nil
	}
	return line[:n-1], nil
}
