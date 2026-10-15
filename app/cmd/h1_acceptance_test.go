//go:build linux

package cmd

// These opt-in tests use only loopback endpoints, synthetic credentials and a
// locally generated CA. The helper process isolates server CPU from clients.
import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/apernet/hysteria/core/v2/client"
	"github.com/apernet/hysteria/core/v2/server"
	"github.com/apernet/hysteria/extras/v2/trafficlogger"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const h1TestAuth = "synthetic-h1-auth-only"

type h1Authenticator struct{}

func (h1Authenticator) Authenticate(_ net.Addr, auth string, _ uint64) (bool, string) {
	return auth == h1TestAuth, "synthetic-session-identity"
}

type h1Settings struct {
	Cert, Key, Ready  string
	Evidence, Capture bool
	IdleSeconds       int
}
type h1Ready struct{ UDP, Control string }
type h1Metrics struct {
	CPUSeconds                                      float64
	TotalAlloc, HeapAlloc, NumGC, PauseNS, LogBytes uint64
	Evidence                                        []server.QUICEvidence
}
type h1Stdout struct{ n atomic.Uint64 }

func (w *h1Stdout) Write(p []byte) (int, error) {
	n, e := os.Stdout.Write(p)
	w.n.Add(uint64(n))
	return n, e
}
func (w *h1Stdout) Sync() error { return nil }

type h1Capture struct {
	mu     sync.Mutex
	events []server.QUICEvidence
	logger serverLogger
}

func (c *h1Capture) LogQUICEvidence(e server.QUICEvidence) {
	c.logger.LogQUICEvidence(e)
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.events) == 256 {
		copy(c.events, c.events[1:])
		c.events = c.events[:255]
	}
	c.events = append(c.events, e)
}

func TestH1AcceptanceServer(t *testing.T) {
	if os.Getenv("PCS_H1_HELPER") != "1" {
		t.Skip("isolated helper only")
	}
	body, err := os.ReadFile(os.Getenv("PCS_H1_SETTINGS"))
	if err != nil {
		t.Fatal(err)
	}
	var cfg h1Settings
	if err = json.Unmarshal(body, &cfg); err != nil {
		t.Fatal(err)
	}
	cert, err := tls.LoadX509KeyPair(cfg.Cert, cfg.Key)
	if err != nil {
		t.Fatal(err)
	}
	udp, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	control, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	sink := &h1Stdout{}
	logger = zap.New(zapcore.NewCore(zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()), sink, zap.InfoLevel))
	capture := &h1Capture{}
	var evidenceLogger server.QUICEvidenceLogger = &serverLogger{}
	if cfg.Capture {
		evidenceLogger = capture
	}
	s, err := server.NewServer(&server.Config{Conn: udp, TLSConfig: server.TLSConfig{Certificates: []tls.Certificate{cert}}, Authenticator: h1Authenticator{}, TrafficLogger: trafficlogger.NewTrafficStatsServer("synthetic-management-only"), QUICConfig: server.QUICConfig{MaxIdleTimeout: time.Duration(cfg.IdleSeconds) * time.Second}, QUICEvidenceConfig: server.QUICEvidenceConfig{Enabled: cfg.Evidence, StreamEvidence: cfg.Evidence, UDPEvidence: cfg.Evidence, Interval: time.Second}, QUICEvidenceLogger: evidenceLogger})
	if err != nil {
		t.Fatal(err)
	}
	go s.Serve()
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		var u syscall.Rusage
		_ = syscall.Getrusage(syscall.RUSAGE_SELF, &u)
		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)
		m := h1Metrics{CPUSeconds: float64(u.Utime.Sec+u.Stime.Sec) + float64(u.Utime.Usec+u.Stime.Usec)/1e6, TotalAlloc: mem.TotalAlloc, HeapAlloc: mem.HeapAlloc, NumGC: uint64(mem.NumGC), PauseNS: mem.PauseTotalNs, LogBytes: sink.n.Load()}
		if cfg.Capture {
			capture.mu.Lock()
			m.Evidence = append(m.Evidence, capture.events...)
			capture.mu.Unlock()
		}
		_ = json.NewEncoder(w).Encode(m)
	})
	httpServer := &http.Server{Handler: mux, ReadHeaderTimeout: time.Second}
	go httpServer.Serve(control)
	body, _ = json.Marshal(h1Ready{UDP: udp.LocalAddr().String(), Control: "http://" + control.Addr().String()})
	if err = os.WriteFile(cfg.Ready, body, 0600); err != nil {
		t.Fatal(err)
	}
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)
	<-stop
	_ = s.Close()
	_ = httpServer.Close()
	_ = logger.Sync()
}

type h1CA struct {
	cert, key string
	roots     *x509.CertPool
}

func h1Certificate(t *testing.T) h1CA {
	t.Helper()
	dir := t.TempDir()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	root := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "H1 isolated test CA"}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, root, root, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	rootCert, _ := x509.ParseCertificate(der)
	pool := x509.NewCertPool()
	pool.AddCert(rootCert)
	leaf := &x509.Certificate{SerialNumber: big.NewInt(2), DNSNames: []string{"pcs-h1.test"}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	leafDER, err := x509.CreateCertificate(rand.Reader, leaf, rootCert, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, _ := x509.MarshalECPrivateKey(key)
	certPath, keyPath := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	if err = os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDER}), 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0600); err != nil {
		t.Fatal(err)
	}
	return h1CA{certPath, keyPath, pool}
}

type h1Worker struct {
	cmd   *exec.Cmd
	ready h1Ready
	once  sync.Once
}

func h1StartWorker(t *testing.T, ca h1CA, evidence, capture bool, idle int) *h1Worker {
	t.Helper()
	dir := t.TempDir()
	cfg := h1Settings{Cert: ca.cert, Key: ca.key, Ready: filepath.Join(dir, "ready.json"), Evidence: evidence, Capture: capture, IdleSeconds: idle}
	path := filepath.Join(dir, "settings.json")
	body, _ := json.Marshal(cfg)
	if err := os.WriteFile(path, body, 0600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestH1AcceptanceServer$", "-test.timeout=10m")
	cmd.Env = append(os.Environ(), "PCS_H1_HELPER=1", "PCS_H1_SETTINGS="+path)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	worker := &h1Worker{cmd: cmd}
	t.Cleanup(worker.close)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if data, err := os.ReadFile(cfg.Ready); err == nil && json.Unmarshal(data, &worker.ready) == nil {
			return worker
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("server helper not ready")
	return nil
}
func (w *h1Worker) close() {
	w.once.Do(func() {
		_ = w.cmd.Process.Signal(syscall.SIGTERM)
		done := make(chan struct{})
		go func() { _ = w.cmd.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			_ = w.cmd.Process.Kill()
			<-done
		}
	})
}
func (w *h1Worker) metrics(t *testing.T) h1Metrics {
	t.Helper()
	httpClient := &http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{Proxy: nil}}
	defer httpClient.CloseIdleConnections()
	resp, err := httpClient.Get(w.ready.Control + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var m h1Metrics
	if err = json.NewDecoder(resp.Body).Decode(&m); err != nil {
		t.Fatal(err)
	}
	return m
}
func h1Client(addr string, ca *x509.CertPool, name, auth string, idle int) (client.Client, error) {
	serverAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}
	c, _, err := client.NewClient(&client.Config{ServerAddr: serverAddr, Auth: auth, TLSConfig: client.TLSConfig{RootCAs: ca, ServerName: name}, QUICConfig: client.QUICConfig{MaxIdleTimeout: time.Duration(idle) * time.Second, KeepAlivePeriod: 2 * time.Second}})
	return c, err
}

// A target writes exactly the requested bytes. Optional gates let tests create
// known outstanding work before interrupting QUIC, rather than calling idle a fault.
func h1Target(t *testing.T, gate <-chan struct{}, entered chan<- struct{}) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(60 * time.Second))
				var size uint64
				if binary.Read(conn, binary.BigEndian, &size) != nil {
					return
				}
				if size > 256<<20 {
					return
				}
				if entered != nil {
					entered <- struct{}{}
				}
				if gate != nil {
					<-gate
				}
				buf := bytes.Repeat([]byte{0x5a}, 32*1024)
				for size > 0 {
					n := min(size, uint64(len(buf)))
					written, err := conn.Write(buf[:n])
					size -= uint64(written)
					if err != nil {
						return
					}
				}
			}()
		}
	}()
	return listener.Addr().String()
}
func h1Read(conn net.Conn, size uint64) error {
	buf := make([]byte, 32*1024)
	for size > 0 {
		n := min(size, uint64(len(buf)))
		if _, err := io.ReadFull(conn, buf[:n]); err != nil {
			return err
		}
		for _, b := range buf[:n] {
			if b != 0x5a {
				return fmt.Errorf("payload mismatch")
			}
		}
		size -= n
	}
	return nil
}
func h1Download(c client.Client, target string, size uint64) (time.Duration, error) {
	started := time.Now()
	conn, err := c.TCP(target)
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(45 * time.Second))
	if err = binary.Write(conn, binary.BigEndian, size); err != nil {
		return 0, err
	}
	err = h1Read(conn, size)
	return time.Since(started), err
}

// This relay changes the server-visible source UDP port without touching host
// firewall/network settings. It also supplies bounded loss and blackout faults.
type h1Relay struct {
	front            *net.UDPConn
	target           *net.UDPAddr
	mu               sync.Mutex
	peer             *net.UDPAddr
	back             *net.UDPConn
	drop             atomic.Bool
	every            atomic.Uint64
	packets, dropped atomic.Uint64
	wg               sync.WaitGroup
	closed           bool
}

func h1NewRelay(t *testing.T, target string) *h1Relay {
	t.Helper()
	addr, err := net.ResolveUDPAddr("udp", target)
	if err != nil {
		t.Fatal(err)
	}
	front, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	r := &h1Relay{front: front, target: addr}
	r.rebind(t)
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		buf := make([]byte, 65536)
		for {
			n, peer, err := front.ReadFromUDP(buf)
			if err != nil {
				return
			}
			r.mu.Lock()
			r.peer = peer
			back := r.back
			r.mu.Unlock()
			if !r.discard() {
				_, _ = back.Write(buf[:n])
			}
		}
	}()
	t.Cleanup(r.close)
	return r
}
func (r *h1Relay) discard() bool {
	n := r.packets.Add(1)
	every := r.every.Load()
	drop := r.drop.Load() || (every > 0 && n%every == 0)
	if drop {
		r.dropped.Add(1)
	}
	return drop
}
func (r *h1Relay) rebind(t *testing.T) {
	t.Helper()
	back, err := net.DialUDP("udp", nil, r.target)
	if err != nil {
		t.Fatal(err)
	}
	r.mu.Lock()
	old := r.back
	r.back = back
	r.mu.Unlock()
	if old != nil {
		_ = old.Close()
	}
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		buf := make([]byte, 65536)
		for {
			n, err := back.Read(buf)
			if err != nil {
				return
			}
			r.mu.Lock()
			peer := r.peer
			r.mu.Unlock()
			if peer != nil && !r.discard() {
				_, _ = r.front.WriteToUDP(buf[:n], peer)
			}
		}
	}()
}
func (r *h1Relay) close() {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.closed = true
	back := r.back
	r.mu.Unlock()
	_ = r.front.Close()
	_ = back.Close()
	r.wg.Wait()
}

func TestH1EvidenceSecurityAndFaults(t *testing.T) {
	if os.Getenv("PCS_H1_ACCEPTANCE") != "1" {
		t.Skip("opt-in isolated acceptance")
	}
	ca := h1Certificate(t)
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("certificate_auth_%v", enabled), func(t *testing.T) {
			w := h1StartWorker(t, ca, enabled, true, 8)
			for _, bad := range []struct {
				roots      *x509.CertPool
				name, auth string
			}{{x509.NewCertPool(), "pcs-h1.test", h1TestAuth}, {ca.roots, "wrong.pcs-h1.test", h1TestAuth}, {ca.roots, "pcs-h1.test", "wrong-synthetic-auth"}} {
				c, err := h1Client(w.ready.UDP, bad.roots, bad.name, bad.auth, 8)
				if c != nil {
					_ = c.Close()
				}
				if err == nil {
					t.Fatal("invalid trust/name/auth accepted")
				}
			}
			if got := w.metrics(t); len(got.Evidence) > 0 {
				t.Fatal("rejected connections emitted evidence")
			}
			c, err := h1Client(w.ready.UDP, ca.roots, "pcs-h1.test", h1TestAuth, 8)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			if _, err = h1Download(c, h1Target(t, nil, nil), 64<<10); err != nil {
				t.Fatal(err)
			}
		})
	}
	t.Run("rebind_loss_and_known_pending_blackout", func(t *testing.T) {
		w := h1StartWorker(t, ca, true, true, 8)
		relay := h1NewRelay(t, w.ready.UDP)
		c, err := h1Client(relay.front.LocalAddr().String(), ca.roots, "pcs-h1.test", h1TestAuth, 8)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		target := h1Target(t, nil, nil)
		if _, err = h1Download(c, target, 64<<10); err != nil {
			t.Fatal(err)
		}
		relay.rebind(t)
		if _, err = h1Download(c, target, 64<<10); err != nil {
			t.Fatalf("source-port change interrupted authenticated connection: %v", err)
		}
		relay.every.Store(100)
		if _, err = h1Download(c, target, 2<<20); err != nil {
			t.Fatalf("bounded packet loss broke transfer: %v", err)
		}
		relay.every.Store(0)
		if relay.dropped.Load() == 0 {
			t.Fatal("loss injector did not drop packets")
		}
		gate := make(chan struct{})
		entered := make(chan struct{}, 1)
		pendingTarget := h1Target(t, gate, entered)
		conn, err := c.TCP(pendingTarget)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(15 * time.Second))
		if err = binary.Write(conn, binary.BigEndian, uint64(8<<20)); err != nil {
			t.Fatal(err)
		}
		<-entered
		relay.drop.Store(true)
		close(gate)
		done := make(chan error, 1)
		go func() { done <- h1Read(conn, 8<<20) }()
		time.Sleep(3 * time.Second)
		metrics := w.metrics(t)
		sawPTO, sawPending, sawAge := false, false, false
		var session string
		for _, e := range metrics.Evidence {
			if e.Session != "" {
				if session == "" {
					session = e.Session
				} else if session != e.Session {
					t.Fatal("rebind created a different authenticated session")
				}
			}
			sawPTO = sawPTO || e.PTOPositiveUpdates > 0
			sawAge = sawAge || e.LastACKAgeMS >= 1000
			if e.Application != nil {
				sawPending = sawPending || e.Application.ToClient.PendingBytes > 0 && e.Application.ToClient.MaxWriteAgeMS >= 1000
			}
		}
		relay.drop.Store(false)
		if err = <-done; err != nil {
			t.Fatalf("temporary blackout did not recover: %v", err)
		}
		_ = conn.Close()
		if !sawPTO || !sawAge || !sawPending {
			t.Fatalf("missing known-blockage evidence: PTO=%v ACK_age=%v pending_write=%v", sawPTO, sawAge, sawPending)
		}
		relay.drop.Store(true)
		deadline := time.Now().Add(11 * time.Second)
		sawIdle := false
		for time.Now().Before(deadline) {
			for _, e := range w.metrics(t).Evidence {
				if e.Event == "closed" && e.CloseClass == "idle_timeout" {
					sawIdle = true
				}
			}
			if sawIdle {
				break
			}
			time.Sleep(200 * time.Millisecond)
		}
		if !sawIdle {
			t.Fatal("configured 8s blackout did not produce typed idle close")
		}
		t.Logf("H1_FAULT_RESULT rebind_same_session=true dropped_datagrams=%d pending_write=true ack_age=true pto_positive=true idle_close=true", relay.dropped.Load())
	})
}

type h1Cgroup struct {
	CPUStat                   map[string]uint64
	MemoryCurrent, MemoryPeak uint64
	CPUQuota, MemoryLimit     string
	Errors                    []string
}

func h1CgroupSnapshot() h1Cgroup {
	out := h1Cgroup{CPUStat: map[string]uint64{}}
	read := func(name string) string {
		b, e := os.ReadFile("/sys/fs/cgroup/" + name)
		if e != nil {
			out.Errors = append(out.Errors, name)
			return ""
		}
		return strings.TrimSpace(string(b))
	}
	for _, line := range strings.Split(read("cpu.stat"), "\n") {
		v := strings.Fields(line)
		if len(v) == 2 {
			out.CPUStat[v[0]], _ = strconv.ParseUint(v[1], 10, 64)
		}
	}
	out.MemoryCurrent, _ = strconv.ParseUint(read("memory.current"), 10, 64)
	out.MemoryPeak, _ = strconv.ParseUint(read("memory.peak"), 10, 64)
	out.CPUQuota = read("cpu.max")
	out.MemoryLimit = read("memory.max")
	return out
}
func h1HarnessCPU() float64 {
	var u syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &u)
	return float64(u.Utime.Sec+u.Stime.Sec) + float64(u.Utime.Usec+u.Stime.Usec)/1e6
}

type h1PerformanceResult struct {
	StartedAt, FinishedAt                                                                                         time.Time
	CgroupBefore, CgroupAfter                                                                                     h1Cgroup
	HarnessCPUSeconds                                                                                             float64
	Workload                                                                                                      string
	Connections, Pair                                                                                             int
	Enabled                                                                                                       bool
	PayloadBytes                                                                                                  uint64
	SmallRequests                                                                                                 int
	WallSeconds, ServerCPUSeconds, MiBPerSecond, CPUSecondsPerMiB, SmallP50MS, SmallP95MS, SmallP99MS, SmallMaxMS float64
	PerConnectionSmallMaxMS                                                                                       []float64
	AllocBytes, GCCount, GCPauseNS, LogBytes                                                                      uint64
}

func h1Percentile(values []float64, p float64) float64 {
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	return sorted[min(len(sorted)-1, int(float64(len(sorted)-1)*p))]
}
func h1PerformanceRun(t *testing.T, ca h1CA, connections, pair int, enabled bool, bulkMiB int) h1PerformanceResult {
	w := h1StartWorker(t, ca, enabled, false, 30)
	defer w.close()
	target := h1Target(t, nil, nil)
	clients := make([]client.Client, connections)
	for i := range clients {
		c, err := h1Client(w.ready.UDP, ca.roots, "pcs-h1.test", h1TestAuth, 30)
		if err != nil {
			t.Fatal(err)
		}
		clients[i] = c
		defer c.Close()
		if _, err = h1Download(c, target, 1024); err != nil {
			t.Fatal(err)
		}
	}
	before := w.metrics(t)
	cgroupBefore := h1CgroupSnapshot()
	harnessBefore := h1HarnessCPU()
	start := time.Now()
	var wg sync.WaitGroup
	var mu sync.Mutex
	var latencies []float64
	perConnection := make([]float64, connections)
	errCh := make(chan error, 3*connections)
	for i, c := range clients {
		if bulkMiB > 0 {
			wg.Add(1)
			go func(c client.Client) {
				defer wg.Done()
				_, err := h1Download(c, target, uint64(bulkMiB<<20)/uint64(connections))
				if err != nil {
					errCh <- err
				}
			}(c)
		}
		for lane := 0; lane < 2; lane++ {
			wg.Add(1)
			go func(i int, c client.Client) {
				defer wg.Done()
				for n := 0; n < 128/connections; n++ {
					elapsed, err := h1Download(c, target, 8<<10)
					if err != nil {
						errCh <- err
						return
					}
					ms := float64(elapsed) / float64(time.Millisecond)
					mu.Lock()
					latencies = append(latencies, ms)
					perConnection[i] = max(perConnection[i], ms)
					mu.Unlock()
				}
			}(i, c)
		}
	}
	wg.Wait()
	wall := time.Since(start).Seconds()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(latencies) != 256 {
		t.Fatal("incomplete small-request denominator")
	}
	// Include asynchronous evidence formatting/output after stream completion in
	// server CPU/alloc accounting, while transfer throughput excludes this grace.
	time.Sleep(100 * time.Millisecond)
	after := w.metrics(t)
	payload := uint64((bulkMiB + 2) << 20)
	workload := "mixed"
	if bulkMiB == 0 {
		workload = "small_only_reference"
	}
	mib := float64(bulkMiB + 2)
	result := h1PerformanceResult{StartedAt: start.UTC(), FinishedAt: time.Now().UTC(), CgroupBefore: cgroupBefore, CgroupAfter: h1CgroupSnapshot(), HarnessCPUSeconds: h1HarnessCPU() - harnessBefore, Workload: workload, Connections: connections, Pair: pair, Enabled: enabled, PayloadBytes: payload, SmallRequests: len(latencies), WallSeconds: wall, ServerCPUSeconds: after.CPUSeconds - before.CPUSeconds, MiBPerSecond: mib / wall, CPUSecondsPerMiB: (after.CPUSeconds - before.CPUSeconds) / mib, SmallP50MS: h1Percentile(latencies, .50), SmallP95MS: h1Percentile(latencies, .95), SmallP99MS: h1Percentile(latencies, .99), SmallMaxMS: h1Percentile(latencies, 1), PerConnectionSmallMaxMS: perConnection, AllocBytes: after.TotalAlloc - before.TotalAlloc, GCCount: after.NumGC - before.NumGC, GCPauseNS: after.PauseNS - before.PauseNS, LogBytes: after.LogBytes - before.LogBytes}
	body, _ := json.Marshal(result)
	t.Logf("H1_PERFORMANCE_RESULT %s", body)
	return result
}
func TestH1EvidencePerformance(t *testing.T) {
	if os.Getenv("PCS_H1_ACCEPTANCE") != "1" {
		t.Skip("opt-in isolated acceptance")
	}
	ca := h1Certificate(t)
	for _, connections := range []int{1, 8} {
		var results []h1PerformanceResult
		for pair := 0; pair < 4; pair++ {
			order := []bool{false, true}
			if pair%2 == 1 {
				order = []bool{true, false}
			}
			for _, enabled := range order {
				results = append(results, h1PerformanceRun(t, ca, connections, pair, enabled, 128))
			}
		}
		for _, enabled := range []bool{false, true} {
			_ = h1PerformanceRun(t, ca, connections, -1, enabled, 0)
		}
		var wall, cpu [2]float64
		for _, r := range results {
			i := 0
			if r.Enabled {
				i = 1
			}
			wall[i] += r.WallSeconds
			cpu[i] += r.ServerCPUSeconds
		}
		throughputRatio := wall[0] / wall[1]
		cpuRatio := cpu[1] / cpu[0]
		t.Logf("H1_GATE connections=%d pairs=4 small_requests_per_mode=1024 throughput_ratio=%.6f server_cpu_per_byte_ratio=%.6f", connections, throughputRatio, cpuRatio)
		if throughputRatio < .95 || cpuRatio > 1.20 {
			t.Errorf("H1 release gate exceeded: connections=%d throughput_ratio=%.4f CPU_per_byte_ratio=%.4f; pause release and analyze", connections, throughputRatio, cpuRatio)
		}
	}
}

func h1UDPEcho(t *testing.T) string {
	t.Helper()
	conn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	go func() {
		buf := make([]byte, 65536)
		for {
			n, addr, err := conn.ReadFrom(buf)
			if err != nil {
				return
			}
			_, _ = conn.WriteTo(buf[:n], addr)
		}
	}()
	return conn.LocalAddr().String()
}
func h1UDPRoundTrip(c client.HyUDPConn, target string, payload []byte) error {
	if err := c.Send(payload, target); err != nil {
		return err
	}
	b, _, err := c.Receive()
	if err != nil {
		return err
	}
	if !bytes.Equal(b, payload) {
		return fmt.Errorf("UDP payload mismatch")
	}
	return nil
}
func TestH1EvidenceUDP(t *testing.T) {
	if os.Getenv("PCS_H1_ACCEPTANCE") != "1" {
		t.Skip("opt-in isolated acceptance")
	}
	ca := h1Certificate(t)
	w := h1StartWorker(t, ca, true, true, 8)
	c, err := h1Client(w.ready.UDP, ca.roots, "pcs-h1.test", h1TestAuth, 8)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	uc, err := c.UDP()
	if err != nil {
		t.Fatal(err)
	}
	defer uc.Close()
	timer := time.AfterFunc(8*time.Second, func() { _ = c.Close() })
	defer timer.Stop()
	target := h1UDPEcho(t)
	for _, size := range []int{1000, 3000} {
		if err = h1UDPRoundTrip(uc, target, bytes.Repeat([]byte{0x5a}, size)); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(1100 * time.Millisecond)
	found := false
	for _, e := range w.metrics(t).Evidence {
		if e.UDP != nil && e.UDP.TargetWrittenBytes == 4000 && e.UDP.TargetReadBytes == 4000 && e.UDP.EnqueuedPayloadBytes == 4000 && e.UDP.FragmentedMessages > 0 && e.UDP.SendTooLarge > 0 && e.UDP.DefragPending > 0 {
			found = true
			if e.PacketEvidence || e.ReceivedPackets != nil || e.CongestionMode != "bbr" || e.NegotiatedServerTx != 0 {
				t.Fatal("cheap-mode/auth coverage wrong")
			}
		}
	}
	if !found {
		t.Fatal("actual UDP/fragment counts absent or duplicate attempts counted as delivered")
	}
}
func TestH1EvidenceUDPPerformance(t *testing.T) {
	if os.Getenv("PCS_H1_ACCEPTANCE") != "1" {
		t.Skip("opt-in isolated acceptance")
	}
	ca := h1Certificate(t)
	target := h1UDPEcho(t)
	for _, connections := range []int{1, 8} {
		var wall, cpu [2]float64
		for pair := 0; pair < 4; pair++ {
			order := []bool{false, true}
			if pair%2 == 1 {
				order = []bool{true, false}
			}
			for _, enabled := range order {
				w := h1StartWorker(t, ca, enabled, false, 30)
				clients := make([]client.Client, connections)
				for i := range clients {
					c, err := h1Client(w.ready.UDP, ca.roots, "pcs-h1.test", h1TestAuth, 30)
					if err != nil {
						t.Fatal(err)
					}
					clients[i] = c
				}
				lanes := make([]client.HyUDPConn, 8)
				for i := range lanes {
					var err error
					lanes[i], err = clients[i%connections].UDP()
					if err != nil {
						t.Fatal(err)
					}
				}
				before := w.metrics(t)
				cb := h1CgroupSnapshot()
				hc := h1HarnessCPU()
				start := time.Now()
				timer := time.AfterFunc(30*time.Second, func() {
					for _, c := range clients {
						_ = c.Close()
					}
				})
				var wg sync.WaitGroup
				errCh := make(chan error, 8)
				var mu sync.Mutex
				var latency []float64
				for _, uc := range lanes {
					wg.Add(1)
					go func(uc client.HyUDPConn) {
						defer wg.Done()
						payload := bytes.Repeat([]byte{0x5a}, 1000)
						for j := 0; j < 2048; j++ {
							at := time.Now()
							if err := h1UDPRoundTrip(uc, target, payload); err != nil {
								errCh <- err
								return
							}
							ms := float64(time.Since(at)) / float64(time.Millisecond)
							mu.Lock()
							latency = append(latency, ms)
							mu.Unlock()
						}
					}(uc)
				}
				wg.Wait()
				elapsed := time.Since(start).Seconds()
				timer.Stop()
				close(errCh)
				for err := range errCh {
					if err != nil {
						t.Fatal(err)
					}
				}
				time.Sleep(100 * time.Millisecond)
				after := w.metrics(t)
				payload := uint64(16384 * 1000 * 2)
				mib := float64(payload) / (1 << 20)
				result := h1PerformanceResult{Workload: "udp_echo_8_sessions", Connections: connections, Pair: pair, Enabled: enabled, StartedAt: start.UTC(), FinishedAt: time.Now().UTC(), CgroupBefore: cb, CgroupAfter: h1CgroupSnapshot(), HarnessCPUSeconds: h1HarnessCPU() - hc, PayloadBytes: payload, SmallRequests: len(latency), WallSeconds: elapsed, ServerCPUSeconds: after.CPUSeconds - before.CPUSeconds, MiBPerSecond: mib / elapsed, CPUSecondsPerMiB: (after.CPUSeconds - before.CPUSeconds) / mib, SmallP50MS: h1Percentile(latency, .5), SmallP95MS: h1Percentile(latency, .95), SmallP99MS: h1Percentile(latency, .99), SmallMaxMS: h1Percentile(latency, 1), AllocBytes: after.TotalAlloc - before.TotalAlloc, GCCount: after.NumGC - before.NumGC, GCPauseNS: after.PauseNS - before.PauseNS, LogBytes: after.LogBytes - before.LogBytes}
				b, _ := json.Marshal(result)
				t.Logf("H1_PERFORMANCE_RESULT %s", b)
				index := 0
				if enabled {
					index = 1
				}
				wall[index] += elapsed
				cpu[index] += result.ServerCPUSeconds
				for _, uc := range lanes {
					_ = uc.Close()
				}
				for _, c := range clients {
					_ = c.Close()
				}
				w.close()
			}
		}
		t.Logf("H1_UDP_GATE connections=%d pairs=4 throughput_ratio=%.6f server_cpu_per_byte_ratio=%.6f", connections, wall[0]/wall[1], cpu[1]/cpu[0])
		if wall[0]/wall[1] < .95 || cpu[1]/cpu[0] > 1.20 {
			t.Errorf("UDP release gate exceeded: connections=%d", connections)
		}
	}
}

// A known queued UDP response distinguishes SendDatagram pressure from an idle
// connection. Packets lost in this injected blackout are not retried by UDP.
func TestH1EvidenceUDPQueueWait(t *testing.T) {
	if os.Getenv("PCS_H1_ACCEPTANCE") != "1" {
		t.Skip("opt-in isolated acceptance")
	}
	ca := h1Certificate(t)
	w := h1StartWorker(t, ca, true, true, 8)
	relay := h1NewRelay(t, w.ready.UDP)
	c, err := h1Client(relay.front.LocalAddr().String(), ca.roots, "pcs-h1.test", h1TestAuth, 8)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	uc, err := c.UDP()
	if err != nil {
		t.Fatal(err)
	}
	defer uc.Close()
	target, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	peers := make(chan net.Addr, 1)
	go func() {
		buf := make([]byte, 1000)
		n, peer, e := target.ReadFrom(buf)
		if e == nil {
			_, _ = target.WriteTo(buf[:n], peer)
			peers <- peer
		}
	}()
	deadline := time.AfterFunc(8*time.Second, func() { _ = c.Close() })
	defer deadline.Stop()
	if err = h1UDPRoundTrip(uc, target.LocalAddr().String(), []byte("synthetic-ready")); err != nil {
		t.Fatal(err)
	}
	peer := <-peers
	relay.drop.Store(true)
	// A bounded loopback burst fills the server application send queue while QUIC
	// cannot receive ACKs. Its socket is in the isolated experiment container.
	burstDone := make(chan struct{})
	go func() {
		defer close(burstDone)
		p := bytes.Repeat([]byte{0x5a}, 1000)
		for i := 0; i < 2048; i++ {
			_, _ = target.WriteTo(p, peer)
		}
	}()
	<-burstDone
	time.Sleep(4500 * time.Millisecond)
	observed := false
	for _, e := range w.metrics(t).Evidence {
		if e.UDP != nil {
			for _, wait := range e.UDP.Waits {
				if wait.Phase == "send_datagram" && wait.PendingBytes > 0 && wait.AgeMS >= 1000 {
					observed = true
				}
			}
		}
	}
	if !observed {
		t.Fatal("known UDP send queue wait absent")
	}
	relay.drop.Store(false)
	time.Sleep(1200 * time.Millisecond)
	completed := false
	for _, e := range w.metrics(t).Evidence {
		if e.UDPOperation != nil && e.UDPOperation.Phase == "send_datagram" && e.UDPOperation.DurationUS >= 1000000 {
			completed = true
		}
	}
	if !completed {
		t.Fatal("recovered slow UDP send completion absent")
	}
	t.Log("H1_UDP_FAULT_RESULT known_pending_send=true completed_slow_send=true receive_queue_drop_coverage=false")
}
