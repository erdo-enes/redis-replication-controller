// Command failover-probe is a tiny Redis write client used to observe, from the
// outside, exactly what an active connection experiences during a controller
// failover.
//
// It behaves like a plain Redis application: it opens ONE long-lived connection
// to a target (normally the redis-write-<set> Service that fronts the current
// master) and issues INCR on a loop. It only reconnects when the connection
// breaks OR the server answers READONLY — which is precisely the behaviour a
// client must have for this controller's label-flip failover to be transparent.
//
// On every state change it logs what happened, so you can read off:
//   - whether the old connection was dropped (hard disconnect) or kept alive but
//     turned read-only (READONLY) when the master was demoted,
//   - the measured write-outage duration (≈ detection + failure threshold +
//     promotion + endpoint propagation + readiness), and
//   - that the backend instance (run_id) actually changed after failover.
//
// It does NOT know or guess the controller's threshold. It just writes and
// measures, so a 5s threshold visibly produces a shorter outage than a 15s one.
package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type config struct {
	addr        string
	interval    time.Duration
	dialTimeout time.Duration
	cmdTimeout  time.Duration
	key         string
	logEvery    int64
}

func loadConfig() config {
	return config{
		addr:        env("TARGET_ADDR", "redis-write-cache.redis.svc.cluster.local:6379"),
		interval:    envDur("PROBE_INTERVAL", 250*time.Millisecond),
		dialTimeout: envDur("DIAL_TIMEOUT", time.Second),
		cmdTimeout:  envDur("CMD_TIMEOUT", time.Second),
		key:         env("PROBE_KEY", "failover-probe:counter"),
		logEvery:    int64(envInt("LOG_EVERY", 20)),
	}
}

type stats struct {
	ok         int64
	readonly   int64
	disconnect int64
	dialErr    int64
}

func (s stats) String() string {
	return fmt.Sprintf("ok=%d readonly=%d disconnect=%d dial-err=%d", s.ok, s.readonly, s.disconnect, s.dialErr)
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.LUTC)
	cfg := loadConfig()
	log.Printf("[START] target=%s interval=%s dial-timeout=%s cmd-timeout=%s key=%q",
		cfg.addr, cfg.interval, cfg.dialTimeout, cfg.cmdTimeout, cfg.key)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	var (
		conn        net.Conn
		reader      *bufio.Reader
		backend     string    // run_id currently serving us
		outageStart time.Time // zero when writes are healthy
		st          stats
	)

	closeConn := func() {
		if conn != nil {
			_ = conn.Close()
			conn, reader = nil, nil
		}
	}
	// noteFailure records the start of an outage on the first failure after OK.
	noteFailure := func() {
		if outageStart.IsZero() {
			outageStart = time.Now()
		}
	}

	ticker := time.NewTicker(cfg.interval)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			log.Printf("[STOP] %s", st)
			closeConn()
			return
		case <-ticker.C:
		}

		// (Re)establish the connection if needed.
		if conn == nil {
			c, r, err := dial(cfg.addr, cfg.dialTimeout)
			if err != nil {
				st.dialErr++
				noteFailure()
				log.Printf("[DIAL-ERR] %v (Service may have no master endpoint during failover)", err)
				continue
			}
			conn, reader = c, r
			id, role := identify(conn, reader, cfg.cmdTimeout)
			switch {
			case backend != "" && id != "" && id != backend:
				log.Printf("[ENDPOINT-CHANGED] now run_id=%s role=%s (previously %s)", short(id), role, short(backend))
			default:
				log.Printf("[CONNECTED] run_id=%s role=%s", short(id), role)
			}
			if id != "" {
				backend = id
			}
		}

		// Write probe.
		start := time.Now()
		n, err := incr(conn, reader, cfg.cmdTimeout, cfg.key)
		rtt := time.Since(start)
		if err != nil {
			noteFailure()
			if isReadOnly(err) {
				st.readonly++
				log.Printf("[READONLY] write rejected: server is now a read-only replica; dropping connection to re-resolve via the Service")
			} else {
				st.disconnect++
				log.Printf("[DISCONNECT] write failed: %v; will reconnect", err)
			}
			closeConn()
			continue
		}

		// Success.
		st.ok++
		if !outageStart.IsZero() {
			log.Printf("[RECOVERED] write outage = %s; writable again on run_id=%s (counter=%d)",
				time.Since(outageStart).Round(time.Millisecond), short(backend), n)
			outageStart = time.Time{}
		}
		if cfg.logEvery > 0 && st.ok%cfg.logEvery == 0 {
			log.Printf("[OK] counter=%d rtt=%s run_id=%s | %s", n, rtt.Round(time.Millisecond), short(backend), st)
		}
	}
}

// --- minimal RESP client (stdlib only) --------------------------------------

type redisError struct{ msg string }

func (e *redisError) Error() string { return e.msg }

func dial(addr string, timeout time.Duration) (net.Conn, *bufio.Reader, error) {
	c, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, nil, err
	}
	return c, bufio.NewReader(c), nil
}

func do(conn net.Conn, r *bufio.Reader, timeout time.Duration, args ...string) (interface{}, error) {
	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		return nil, err
	}
	if err := writeCmd(conn, args...); err != nil {
		return nil, err
	}
	return readReply(r)
}

func incr(conn net.Conn, r *bufio.Reader, timeout time.Duration, key string) (int64, error) {
	reply, err := do(conn, r, timeout, "INCR", key)
	if err != nil {
		return 0, err
	}
	if n, ok := reply.(int64); ok {
		return n, nil
	}
	return 0, fmt.Errorf("unexpected INCR reply %T", reply)
}

// identify returns the server's run_id and replication role via INFO, so a
// change of backend instance after failover is visible.
func identify(conn net.Conn, r *bufio.Reader, timeout time.Duration) (runID, role string) {
	reply, err := do(conn, r, timeout, "INFO")
	if err != nil {
		return "", ""
	}
	s, _ := reply.(string)
	return infoField(s, "run_id:"), infoField(s, "role:")
}

func infoField(info, prefix string) string {
	for _, ln := range strings.Split(info, "\n") {
		ln = strings.TrimSpace(ln)
		if strings.HasPrefix(ln, prefix) {
			return strings.TrimSpace(ln[len(prefix):])
		}
	}
	return ""
}

func isReadOnly(err error) bool {
	var re *redisError
	if errors.As(err, &re) {
		return strings.HasPrefix(re.msg, "READONLY")
	}
	return false
}

func writeCmd(w io.Writer, args ...string) error {
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

func readReply(r *bufio.Reader) (interface{}, error) {
	line, err := readLine(r)
	if err != nil {
		return nil, err
	}
	if len(line) == 0 {
		return nil, errors.New("empty reply")
	}
	switch line[0] {
	case '+':
		return string(line[1:]), nil
	case '-':
		return nil, &redisError{msg: string(line[1:])}
	case ':':
		return strconv.ParseInt(string(line[1:]), 10, 64)
	case '$':
		n, err := strconv.Atoi(string(line[1:]))
		if err != nil {
			return nil, fmt.Errorf("bad bulk length %q: %w", line[1:], err)
		}
		if n < 0 {
			return nil, nil
		}
		buf := make([]byte, n+2) // include trailing CRLF
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		return string(buf[:n]), nil
	case '*':
		n, err := strconv.Atoi(string(line[1:]))
		if err != nil {
			return nil, fmt.Errorf("bad array length %q: %w", line[1:], err)
		}
		if n < 0 {
			return nil, nil
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
	}
	return nil, fmt.Errorf("unknown reply type %q", line[0])
}

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

// --- env helpers -------------------------------------------------------------

func short(id string) string {
	if len(id) > 12 {
		return id[:12]
	}
	if id == "" {
		return "?"
	}
	return id
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envDur(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
		log.Printf("[WARN] invalid %s=%q, using %s", key, v, def)
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
		log.Printf("[WARN] invalid %s=%q, using %d", key, v, def)
	}
	return def
}
