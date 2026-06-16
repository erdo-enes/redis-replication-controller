// Command failover-probe is a Redis client that behaves like a real
// application: it maintains ONE long-lived connection to the redis-write-<set>
// Service and, on each tick, performs both a READ (GET a key matching the
// data-loader prefix) and a WRITE (INCR a counter). It reconnects only when the
// connection breaks or the server replies READONLY.
//
// It is designed to work alongside the data-loader Job: after that Job fills
// Redis with realistic keys prefixed by DATA_PREFIX, this probe reads those
// keys and writes fresh data, measuring exactly what an active connection
// experiences during a controller failover.
//
// On every state change it logs:
//   - whether the old connection was dropped (hard disconnect) or kept alive but
//     turned read-only (READONLY) when the master was demoted,
//   - the measured write-outage duration,
//   - that the backend instance (run_id) actually changed after failover.
package main

import (
	"bufio"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"log"
	"math/big"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
)

type config struct {
	addr        string
	interval    time.Duration
	dialTimeout time.Duration
	cmdTimeout  time.Duration
	probeKey    string
	dataPrefix  string
	logEvery    int64
}

func loadConfig() config {
	return config{
		addr:        env("TARGET_ADDR", "redis-write-cache.redis-test-enes.svc.cluster.local:6379"),
		interval:    envDur("PROBE_INTERVAL", 250*time.Millisecond),
		dialTimeout: envDur("DIAL_TIMEOUT", time.Second),
		cmdTimeout:  envDur("CMD_TIMEOUT", time.Second),
		probeKey:    env("PROBE_KEY", "failover-probe:counter"),
		dataPrefix:  env("DATA_PREFIX", "ld"),
		logEvery:    int64(envInt("LOG_EVERY", 20)),
	}
}

type stats struct {
	ok         int64
	readonly   int64
	disconnect int64
	dialErr    int64
	readOK     int64
	readMiss   int64
}

func (s stats) String() string {
	return fmt.Sprintf("ok=%d readonly=%d disconnect=%d dial-err=%d read-ok=%d read-miss=%d",
		s.ok, s.readonly, s.disconnect, s.dialErr, s.readOK, s.readMiss)
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.LUTC)
	cfg := loadConfig()
	log.Printf("[START] target=%s interval=%s dial-timeout=%s cmd-timeout=%s counter-key=%q data-prefix=%q",
		cfg.addr, cfg.interval, cfg.dialTimeout, cfg.cmdTimeout, cfg.probeKey, cfg.dataPrefix)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	var (
		conn        net.Conn
		reader      *bufio.Reader
		backend     string
		outageStart time.Time
		st          stats
		tick        int64
	)

	// Build a pool of sample keys to read. We generate keys matching the exact
	// patterns the data-loader writes so GET hits existing data.
	sampleKeys := buildSampleKeys(cfg.dataPrefix, 200)

	closeConn := func() {
		if conn != nil {
			_ = conn.Close()
			conn, reader = nil, nil
		}
	}
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

		tick++

		// --- (Re)establish connection if needed ---
		if conn == nil {
			c, r, err := dial(cfg.addr, cfg.dialTimeout)
			if err != nil {
				st.dialErr++
				noteFailure()
				log.Printf("[DIAL-ERR] %v (Service may have no master endpoint during failover)", err)
				continue
			}
			conn, reader = c, r

			// On first connect (or after endpoint change), discover the data
			// keys actually present via SCAN so reads hit real keys.
			id, role, dbSize := identify(conn, reader, cfg.cmdTimeout)
			switch {
			case backend != "" && id != "" && id != backend:
				log.Printf("[ENDPOINT-CHANGED] now run_id=%s role=%s (previously %s)", short(id), role, short(backend))
			case backend == "":
				log.Printf("[CONNECTED] run_id=%s role=%s dbsize=%d", short(id), role, dbSize)
			default:
				log.Printf("[RECONNECTED] run_id=%s role=%s dbsize=%d", short(id), role, dbSize)
			}
			if id != "" {
				backend = id
			}

			// Scan for real keys matching our prefix to use as read targets.
			discovered := scanKeys(conn, reader, cfg.cmdTimeout, cfg.dataPrefix, 100)
			if len(discovered) > 0 {
				sampleKeys = discovered
				log.Printf("[SCAN] discovered %d keys matching prefix=%q", len(discovered), cfg.dataPrefix)
			}
		}

		// --- READ: GET a key from the data-loader set ---
		readKey := sampleKeys[int(tick)%len(sampleKeys)]
		readStart := time.Now()
		val, err := get(conn, reader, cfg.cmdTimeout, readKey)
		readRTT := time.Since(readStart)

		if err != nil {
			noteFailure()
			if isReadOnly(err) {
				st.readonly++
				log.Printf("[READONLY] GET %s rejected: server is now a read-only replica; dropping connection", readKey)
			} else {
				st.disconnect++
				log.Printf("[DISCONNECT] GET %s failed: %v; will reconnect", readKey, err)
			}
			closeConn()
			continue
		}
		if val != "" {
			st.readOK++
		} else {
			st.readMiss++
		}

		// --- WRITE: INCR counter ---
		writeStart := time.Now()
		n, err := incr(conn, reader, cfg.cmdTimeout, cfg.probeKey)
		writeRTT := time.Since(writeStart)
		if err != nil {
			noteFailure()
			if isReadOnly(err) {
				st.readonly++
				log.Printf("[READONLY] write rejected: server became read-only after read; dropping connection")
			} else {
				st.disconnect++
				log.Printf("[DISCONNECT] write failed: %v; will reconnect", err)
			}
			closeConn()
			continue
		}

		// --- Success ---
		st.ok++
		if !outageStart.IsZero() {
			log.Printf("[RECOVERED] write outage = %s; writable again on run_id=%s (counter=%d)",
				time.Since(outageStart).Round(time.Millisecond), short(backend), n)
			outageStart = time.Time{}
		}
		if cfg.logEvery > 0 && st.ok%cfg.logEvery == 0 {
			log.Printf("[OK] counter=%d read-rtt=%s write-rtt=%s key=%q val-len=%d run_id=%s | %s",
				n, readRTT.Round(time.Microsecond), writeRTT.Round(time.Microsecond),
				readKey, len(val), short(backend), st)
		}
	}
}

// buildSampleKeys generates a set of keys matching the exact patterns the
// data-loader writes. Used as fallback read targets before SCAN discovers
// real keys.
func buildSampleKeys(prefix string, count int) []string {
	patterns := []struct {
		fmt   string
		args  func(i int) []interface{}
	}{
		{fmt: "%s:session:", args: func(i int) []interface{} { return []interface{}{prefix} }},
		{fmt: "%s:user:usr_%d:profile", args: func(i int) []interface{} { return []interface{}{prefix, 100000 + i%900000} }},
		{fmt: "%s:cache:product:prod_%d", args: func(i int) []interface{} { return []interface{}{prefix, 1000 + i%9000} }},
		{fmt: "%s:cache:category:", args: func(i int) []interface{} { return []interface{}{prefix} }},
		{fmt: "%s:cache:api:", args: func(i int) []interface{} { return []interface{}{prefix} }},
		{fmt: "%s:ratelimit:", args: func(i int) []interface{} { return []interface{}{prefix} }},
		{fmt: "%s:feature:", args: func(i int) []interface{} { return []interface{}{prefix} }},
		{fmt: "%s:lb:sticky:", args: func(i int) []interface{} { return []interface{}{prefix} }},
	}
	keys := make([]string, 0, count)
	for i := 0; i < count; i++ {
		p := patterns[i%len(patterns)]
		k := fmt.Sprintf(p.fmt, p.args(i)...)
		if !strings.HasSuffix(k, ":") {
			keys = append(keys, k)
		} else {
			// For patterns ending with ":", append a random UUID segment
			keys = append(keys, k+randomHex(8))
		}
	}
	return keys
}

// scanKeys uses SCAN to discover up to limit keys matching prefix.
func scanKeys(conn net.Conn, r *bufio.Reader, timeout time.Duration, prefix string, limit int) []string {
	var keys []string
	cursor := "0"
	for {
		reply, err := do(conn, r, timeout, "SCAN", cursor, "MATCH", prefix+":*", "COUNT", "50")
		if err != nil {
			return keys
		}
		arr, ok := reply.([]interface{})
		if !ok || len(arr) < 2 {
			return keys
		}
		cursor, _ = arr[0].(string)
		if keyArr, ok := arr[1].([]interface{}); ok {
			for _, k := range keyArr {
				if s, ok := k.(string); ok {
					keys = append(keys, s)
					if len(keys) >= limit {
						return keys
					}
				}
			}
		}
		if cursor == "0" {
			break
		}
	}
	return keys
}

// --- minimal RESP client (stdlib only) ---

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

func get(conn net.Conn, r *bufio.Reader, timeout time.Duration, key string) (string, error) {
	reply, err := do(conn, r, timeout, "GET", key)
	if err != nil {
		return "", err
	}
	if reply == nil {
		return "", nil
	}
	if s, ok := reply.(string); ok {
		return s, nil
	}
	return "", fmt.Errorf("unexpected GET reply %T", reply)
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

func identify(conn net.Conn, r *bufio.Reader, timeout time.Duration) (runID, role string, dbSize int64) {
	reply, err := do(conn, r, timeout, "INFO")
	if err != nil {
		return "", "", 0
	}
	s, _ := reply.(string)
	for _, ln := range strings.Split(s, "\n") {
		ln = strings.TrimSpace(ln)
		if strings.HasPrefix(ln, "db0:keys=") {
			dbSize, _ = strconv.ParseInt(strings.SplitN(ln[len("db0:keys="):], ",", 2)[0], 10, 64)
		}
	}
	return infoField(s, "run_id:"), infoField(s, "role:"), dbSize
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
		buf := make([]byte, n+2)
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

// --- helpers ---

var rnd = mustRand()

type randReader struct{}

func (r *randReader) Int(max int) int {
	n, _ := rand.Int(rand.Reader, big.NewInt(int64(max)))
	return int(n.Int64())
}

func mustRand() *randReader { return &randReader{} }

func randomHex(n int) string {
	b := make([]byte, n/2+1)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%x", b)[:n]
}

var tickCounter atomic.Int64

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
