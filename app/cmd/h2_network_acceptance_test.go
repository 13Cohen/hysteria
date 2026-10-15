//go:build linux

package cmd

// Opt-in controlled network experiments. The test binary is always the baseline
// client; PCS_H1_SERVER_BINARY selects the server-only candidate. All endpoints
// live in one Docker --network none namespace. No host networking is modified.
import (
	"bytes"
	"container/heap"
	"encoding/binary"
	"encoding/json"
	"github.com/apernet/hysteria/core/v2/client"
	"github.com/apernet/hysteria/core/v2/server"
	"net"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type h2Packet struct {
	at   time.Time
	body []byte
	send func([]byte) error
}
type h2Heap []h2Packet

func (h h2Heap) Len() int           { return len(h) }
func (h h2Heap) Less(i, j int) bool { return h[i].at.Before(h[j].at) }
func (h h2Heap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *h2Heap) Push(x any)        { *h = append(*h, x.(h2Packet)) }
func (h *h2Heap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	old[n-1] = h2Packet{}
	*h = old[:n-1]
	return x
}

type h2LinkStats struct {
	SubmittedPackets, SubmittedBytes, ForwardedPackets, ForwardedBytes, QueueDrops, InjectedDrops, ReorderDelays, WriteErrors, SchedulerOverflow uint64
	MaxQueueBytes, MaxSchedulerLatenessUS                                                                                                        int64
	PendingPackets                                                                                                                               int
}
type h2Link struct {
	mu       sync.Mutex
	rate     int64
	next     time.Time
	q        h2Heap
	stats    h2LinkStats
	impaired bool
	wake     chan struct{}
	stop     chan struct{}
	done     chan struct{}
	once     sync.Once
	ticks    []int64
}

func h2NewLink(rate int64) *h2Link {
	l := &h2Link{rate: rate, wake: make(chan struct{}, 1), stop: make(chan struct{}), done: make(chan struct{})}
	go l.run()
	return l
}
func (l *h2Link) enqueue(body []byte, send func([]byte) error) {
	now := time.Now()
	l.mu.Lock()
	defer l.mu.Unlock()
	l.stats.SubmittedPackets++
	l.stats.SubmittedBytes += uint64(len(body))
	index := l.stats.SubmittedPackets
	queued := int64(0)
	if l.next.After(now) {
		queued = int64(l.next.Sub(now)) * l.rate / int64(time.Second)
	}
	cost := int64(len(body) + 28)
	if queued+cost > 65536 {
		l.stats.QueueDrops++
		return
	}
	l.stats.MaxQueueBytes = max(l.stats.MaxQueueBytes, queued+cost)
	if now.After(l.next) {
		l.next = now
	}
	l.next = l.next.Add(time.Duration(cost * int64(time.Second) / l.rate))
	if l.impaired && index%200 == 0 {
		l.stats.InjectedDrops++
		return
	}
	at := l.next.Add(80 * time.Millisecond)
	if l.impaired && index%97 == 0 {
		at = at.Add(40 * time.Millisecond)
		l.stats.ReorderDelays++
	}
	if len(l.q) >= 2048 {
		l.stats.SchedulerOverflow++
		return
	}
	heap.Push(&l.q, h2Packet{at: at, body: append([]byte(nil), body...), send: send})
	select {
	case l.wake <- struct{}{}:
	default:
	}
}
func (l *h2Link) run() {
	defer close(l.done)
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	for {
		l.mu.Lock()
		wait := time.Hour
		if len(l.q) > 0 {
			wait = time.Until(l.q[0].at)
			if wait < 0 {
				wait = 0
			}
		}
		l.mu.Unlock()
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(wait)
		select {
		case <-l.stop:
			return
		case <-l.wake:
			continue
		case <-timer.C:
		}
		for {
			l.mu.Lock()
			if len(l.q) == 0 || time.Now().Before(l.q[0].at) {
				l.mu.Unlock()
				break
			}
			p := heap.Pop(&l.q).(h2Packet)
			lag := time.Since(p.at).Microseconds()
			l.stats.MaxSchedulerLatenessUS = max(l.stats.MaxSchedulerLatenessUS, lag)
			if len(l.ticks) < 200000 {
				l.ticks = append(l.ticks, lag)
			}
			l.mu.Unlock()
			err := p.send(p.body)
			l.mu.Lock()
			if err != nil {
				l.stats.WriteErrors++
			} else {
				l.stats.ForwardedPackets++
				l.stats.ForwardedBytes += uint64(len(p.body))
			}
			l.mu.Unlock()
		}
	}
}
func (l *h2Link) snapshot() h2LinkStats {
	l.mu.Lock()
	defer l.mu.Unlock()
	s := l.stats
	s.PendingPackets = len(l.q)
	return s
}
func (l *h2Link) lagP95() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	a := append([]int64(nil), l.ticks...)
	if len(a) == 0 {
		return 0
	}
	sort.Slice(a, func(i, j int) bool { return a[i] < a[j] })
	return a[(len(a)-1)*95/100]
}
func (l *h2Link) enable() { l.mu.Lock(); l.impaired = true; l.mu.Unlock() }
func (l *h2Link) close()  { l.once.Do(func() { close(l.stop); <-l.done }) }
func h2LinkDelta(a, b h2LinkStats) h2LinkStats {
	return h2LinkStats{SubmittedPackets: b.SubmittedPackets - a.SubmittedPackets, SubmittedBytes: b.SubmittedBytes - a.SubmittedBytes, ForwardedPackets: b.ForwardedPackets - a.ForwardedPackets, ForwardedBytes: b.ForwardedBytes - a.ForwardedBytes, QueueDrops: b.QueueDrops - a.QueueDrops, InjectedDrops: b.InjectedDrops - a.InjectedDrops, ReorderDelays: b.ReorderDelays - a.ReorderDelays, WriteErrors: b.WriteErrors - a.WriteErrors, SchedulerOverflow: b.SchedulerOverflow - a.SchedulerOverflow, MaxQueueBytes: b.MaxQueueBytes, MaxSchedulerLatenessUS: b.MaxSchedulerLatenessUS, PendingPackets: b.PendingPackets}
}

type h2Relay struct {
	front, back *net.UDPConn
	mu          sync.Mutex
	peer        *net.UDPAddr
	up, down    *h2Link
	wg          sync.WaitGroup
}

func h2NewRelay(t *testing.T, target string, up, down *h2Link) *h2Relay {
	t.Helper()
	addr, err := net.ResolveUDPAddr("udp", target)
	if err != nil {
		t.Fatal(err)
	}
	front, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	back, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		front.Close()
		t.Fatal(err)
	}
	r := &h2Relay{front: front, back: back, up: up, down: down}
	r.wg.Add(2)
	go func() {
		defer r.wg.Done()
		b := make([]byte, 65536)
		for {
			n, peer, e := front.ReadFromUDP(b)
			if e != nil {
				return
			}
			r.mu.Lock()
			r.peer = peer
			r.mu.Unlock()
			up.enqueue(b[:n], func(p []byte) error { _, e := back.Write(p); return e })
		}
	}()
	go func() {
		defer r.wg.Done()
		b := make([]byte, 65536)
		for {
			n, e := back.Read(b)
			if e != nil {
				return
			}
			r.mu.Lock()
			peer := r.peer
			r.mu.Unlock()
			if peer != nil {
				peerCopy := *peer
				down.enqueue(b[:n], func(p []byte) error { _, e := front.WriteToUDP(p, &peerCopy); return e })
			}
		}
	}()
	t.Cleanup(func() { front.Close(); back.Close(); r.wg.Wait() })
	return r
}

type h2TargetStats struct{ offered, writeErrors atomic.Uint64 }

func h2StreamingTarget(t *testing.T, start <-chan struct{}, ready chan<- struct{}) (string, *h2TargetStats) {
	t.Helper()
	conn, e := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if e != nil {
		t.Fatal(e)
	}
	stop := make(chan struct{})
	s := &h2TargetStats{}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		b := make([]byte, 64)
		for {
			n, peer, e := conn.ReadFromUDP(b)
			if e != nil {
				return
			}
			if n != 12 {
				continue
			}
			id := binary.BigEndian.Uint32(b[:4])
			count := binary.BigEndian.Uint32(b[4:8])
			ms := binary.BigEndian.Uint32(b[8:12])
			if count == 0 || count > 3600 || ms != 12000 {
				continue
			}
			dst := *peer
			wg.Add(1)
			go func() {
				defer wg.Done()
				ready <- struct{}{}
				select {
				case <-stop:
					return
				case <-start:
				}
				begin := time.Now()
				body := make([]byte, 1350)
				for seq := uint32(0); seq < count; seq++ {
					at := begin.Add(time.Duration(seq) * time.Duration(ms) * time.Millisecond / time.Duration(count))
					timer := time.NewTimer(time.Until(at))
					select {
					case <-stop:
						timer.Stop()
						return
					case <-timer.C:
					}
					binary.BigEndian.PutUint32(body[:4], id)
					binary.BigEndian.PutUint32(body[4:8], seq)
					for i := 8; i < len(body); i++ {
						body[i] = byte((id*31 + seq) % 251)
					}
					if _, e := conn.WriteToUDP(body, &dst); e != nil {
						s.writeErrors.Add(1)
					} else {
						s.offered.Add(1)
					}
				}
			}()
		}
	}()
	t.Cleanup(func() { close(stop); conn.Close(); wg.Wait() })
	return conn.LocalAddr().String(), s
}

type h2EvidenceTotals struct {
	ACK, Lost, TimeLoss, ReorderLoss, PTO, UDPRead, UDPQueued, UDPFragments, UDPTooLarge, UDPWriteErrors, UDPSendErrors, Untracked, LogDrops uint64
	RTTMinUS, RTTMedianUS                                                                                                                    int64
}

func h2Totals(events []server.QUICEvidence) h2EvidenceTotals {
	latest := map[string]server.QUICEvidence{}
	for _, e := range events {
		if e.Time.After(latest[e.Session].Time) {
			latest[e.Session] = e
		}
	}
	var out h2EvidenceTotals
	var rtts []int64
	for _, e := range latest {
		out.ACK += e.AcknowledgedPackets
		out.Lost += e.LostPackets
		out.TimeLoss += e.TimeThresholdLosses
		out.ReorderLoss += e.ReorderingThresholdLosses
		out.PTO += e.PTOPositiveUpdates
		out.LogDrops = max(out.LogDrops, e.DroppedEvents)
		out.Untracked = max(out.Untracked, e.UntrackedConnections)
		if e.LatestRTTUS > 0 {
			rtts = append(rtts, e.LatestRTTUS)
		}
		if e.UDP != nil {
			out.UDPRead += e.UDP.TargetReadBytes
			out.UDPQueued += e.UDP.EnqueuedPayloadBytes
			out.UDPFragments += e.UDP.FragmentedMessages
			out.UDPTooLarge += e.UDP.SendTooLarge
			out.UDPWriteErrors += e.UDP.TargetWriteErrors
			out.UDPSendErrors += e.UDP.SendErrors
		}
	}
	if len(rtts) > 0 {
		sort.Slice(rtts, func(i, j int) bool { return rtts[i] < rtts[j] })
		out.RTTMinUS = rtts[0]
		out.RTTMedianUS = rtts[len(rtts)/2]
	}
	return out
}
func h2EvidenceDelta(a, b h2EvidenceTotals) h2EvidenceTotals {
	return h2EvidenceTotals{ACK: b.ACK - a.ACK, Lost: b.Lost - a.Lost, TimeLoss: b.TimeLoss - a.TimeLoss, ReorderLoss: b.ReorderLoss - a.ReorderLoss, PTO: b.PTO - a.PTO, UDPRead: b.UDPRead - a.UDPRead, UDPQueued: b.UDPQueued - a.UDPQueued, UDPFragments: b.UDPFragments - a.UDPFragments, UDPTooLarge: b.UDPTooLarge - a.UDPTooLarge, UDPWriteErrors: b.UDPWriteErrors - a.UDPWriteErrors, UDPSendErrors: b.UDPSendErrors - a.UDPSendErrors, Untracked: b.Untracked, LogDrops: b.LogDrops, RTTMinUS: b.RTTMinUS, RTTMedianUS: b.RTTMedianUS}
}

type h2NetworkResult struct {
	Variant, Profile                                                                                                                    string
	Connections                                                                                                                         int
	StartedAt, FinishedAt                                                                                                               time.Time
	WarmupTCPMillis                                                                                                                     []float64
	TCPDeliveredBytes, UDPReceivedDatagrams, UDPReceivedBytes, UDPOfferedDatagrams, UDPInvalid, UDPDuplicates, TargetWriteErrors        uint64
	TCPCompletionSeconds, WallSeconds, ServerCPUSeconds, HarnessAndSimulatorCPUSeconds, SmallP50MS, SmallP95MS, ServerCPUPerReceivedMiB float64
	TCPFailures, UDPReceiveErrors                                                                                                       int
	Up, Down                                                                                                                            h2LinkStats
	UpSchedulerP95US, DownSchedulerP95US                                                                                                int64
	Evidence                                                                                                                            h2EvidenceTotals
	CgroupBefore, CgroupAfter                                                                                                           h1Cgroup
	SimulatorLocation                                                                                                                   string
}

func TestH2ControlledNetwork(t *testing.T) {
	if os.Getenv("PCS_H2_NETWORK") != "1" {
		t.Skip("opt-in controlled network")
	}
	connections, e := strconv.Atoi(os.Getenv("PCS_H2_CONNECTIONS"))
	if e != nil || (connections != 1 && connections != 4) {
		t.Fatal("connections must be1 or4")
	}
	profile := os.Getenv("PCS_H2_PROFILE")
	if profile != "clean_tcp" && profile != "loss_reorder_mixed" {
		t.Fatal("unknown profile")
	}
	ca := h1Certificate(t)
	w := h1StartWorker(t, ca, true, true, 60)
	defer w.close()
	up, down := h2NewLink(250000), h2NewLink(500000)
	defer up.close()
	defer down.close()
	target := h1Target(t, nil, nil)
	echo := h1UDPEcho(t)
	cs := make([]client.Client, connections)
	warm := []float64{}
	for i := range cs {
		r := h2NewRelay(t, w.ready.UDP, up, down)
		c, err := h1Client(r.front.LocalAddr().String(), ca.roots, "pcs-h1.test", h1TestAuth, 60)
		if err != nil {
			t.Fatal(err)
		}
		cs[i] = c
		defer c.Close()
		duration, err := h1Download(c, target, 1024)
		if err != nil {
			t.Fatal(err)
		}
		warm = append(warm, float64(duration)/float64(time.Millisecond))
		if duration < 130*time.Millisecond {
			t.Fatal("160ms RTT link inactive")
		}
		u, err := c.UDP()
		if err != nil {
			t.Fatal(err)
		}
		if err = h1UDPRoundTrip(u, echo, bytes.Repeat([]byte{0x5a}, 1350)); err != nil {
			t.Fatal(err)
		}
		u.Close()
	}
	var recv, invalid, duplicates atomic.Uint64
	var recvErr atomic.Int64
	var udpWG sync.WaitGroup
	var targetStats *h2TargetStats
	var us []client.HyUDPConn
	udpStart := make(chan struct{})
	udpReady := make(chan struct{}, connections*2)
	if profile == "loss_reorder_mixed" {
		dst, stats := h2StreamingTarget(t, udpStart, udpReady)
		targetStats = stats
		for i, c := range cs {
			for lane := 0; lane < 2; lane++ {
				u, err := c.UDP()
				if err != nil {
					t.Fatal(err)
				}
				us = append(us, u)
				id := uint32(i*2 + lane + 1)
				count := uint32(3600 / (connections * 2))
				udpWG.Add(1)
				go func(u client.HyUDPConn, id, count uint32) {
					defer udpWG.Done()
					seen := make([]bool, count)
					for {
						b, _, err := u.Receive()
						if err != nil {
							return
						}
						if len(b) != 1350 || binary.BigEndian.Uint32(b[:4]) != id {
							invalid.Add(1)
							continue
						}
						seq := binary.BigEndian.Uint32(b[4:8])
						if seq >= count {
							invalid.Add(1)
							continue
						}
						valid := true
						for _, v := range b[8:] {
							if v != byte((id*31+seq)%251) {
								valid = false
								break
							}
						}
						if !valid {
							invalid.Add(1)
							continue
						}
						if seen[seq] {
							duplicates.Add(1)
							continue
						}
						seen[seq] = true
						recv.Add(1)
					}
				}(u, id, count)
				command := make([]byte, 12)
				binary.BigEndian.PutUint32(command[:4], id)
				binary.BigEndian.PutUint32(command[4:8], count)
				binary.BigEndian.PutUint32(command[8:12], 12000)
				if err := u.Send(command, dst); err != nil {
					t.Fatal(err)
				}
			}
		}
		for range us {
			select {
			case <-udpReady:
			case <-time.After(3 * time.Second):
				t.Fatal("synthetic UDP target was not primed")
			}
		}
	}
	time.Sleep(1100 * time.Millisecond)
	before := w.metrics(t)
	cgBefore := h1CgroupSnapshot()
	cpuBefore := h1HarnessCPU()
	upBefore, downBefore := up.snapshot(), down.snapshot()
	evBefore := h2Totals(before.Evidence)
	if profile == "loss_reorder_mixed" {
		up.enable()
		down.enable()
	}
	begin := time.Now()
	close(udpStart)
	var mu sync.Mutex
	var latencies []float64
	var tcpBytes atomic.Uint64
	var tcpFailures atomic.Int64
	var tcpWG sync.WaitGroup
	var tcpWall float64
	for _, c := range cs {
		tcpWG.Add(2)
		go func(c client.Client) {
			defer tcpWG.Done()
			n := uint64(2<<20) / uint64(connections)
			_, err := h1Download(c, target, n)
			if err != nil {
				tcpFailures.Add(1)
			} else {
				tcpBytes.Add(n)
			}
		}(c)
		go func(c client.Client) {
			defer tcpWG.Done()
			for i := 0; i < 16/connections; i++ {
				d, err := h1Download(c, target, 8192)
				if err != nil {
					tcpFailures.Add(1)
					return
				}
				tcpBytes.Add(8192)
				mu.Lock()
				latencies = append(latencies, float64(d)/float64(time.Millisecond))
				mu.Unlock()
			}
		}(c)
	}
	tcpDone := make(chan struct{})
	go func() { tcpWG.Wait(); tcpWall = time.Since(begin).Seconds(); close(tcpDone) }()
	if profile == "loss_reorder_mixed" {
		if delay := time.Until(begin.Add(14 * time.Second)); delay > 0 {
			time.Sleep(delay)
		}
		for _, u := range us {
			u.Close()
		}
		udpWG.Wait()
	}
	<-tcpDone
	wall := time.Since(begin).Seconds()
	time.Sleep(1100 * time.Millisecond)
	after := w.metrics(t)
	ev := h2EvidenceDelta(evBefore, h2Totals(after.Evidence))
	total := tcpBytes.Load() + recv.Load()*1350
	result := h2NetworkResult{Variant: os.Getenv("PCS_H2_VARIANT"), Profile: profile, Connections: connections, StartedAt: begin.UTC(), FinishedAt: time.Now().UTC(), WarmupTCPMillis: warm, TCPDeliveredBytes: tcpBytes.Load(), UDPReceivedDatagrams: recv.Load(), UDPReceivedBytes: recv.Load() * 1350, UDPInvalid: invalid.Load(), UDPDuplicates: duplicates.Load(), TCPCompletionSeconds: tcpWall, WallSeconds: wall, ServerCPUSeconds: after.CPUSeconds - before.CPUSeconds, HarnessAndSimulatorCPUSeconds: h1HarnessCPU() - cpuBefore, TCPFailures: int(tcpFailures.Load()), UDPReceiveErrors: int(recvErr.Load()), Up: h2LinkDelta(upBefore, up.snapshot()), Down: h2LinkDelta(downBefore, down.snapshot()), UpSchedulerP95US: up.lagP95(), DownSchedulerP95US: down.lagP95(), Evidence: ev, CgroupBefore: cgBefore, CgroupAfter: h1CgroupSnapshot(), SimulatorLocation: "baseline client/harness process; server separate process, same limited cgroup"}
	if len(latencies) > 0 {
		result.SmallP50MS = h1Percentile(latencies, .5)
		result.SmallP95MS = h1Percentile(latencies, .95)
	}
	if total > 0 {
		result.ServerCPUPerReceivedMiB = result.ServerCPUSeconds / (float64(total) / (1 << 20))
	}
	if targetStats != nil {
		result.UDPOfferedDatagrams = targetStats.offered.Load()
		result.TargetWriteErrors = targetStats.writeErrors.Load()
	}
	body, _ := json.Marshal(result)
	t.Logf("H2_NETWORK_RESULT %s", body)
	if result.TCPFailures != 0 || result.TCPDeliveredBytes != (2<<20)+16*8192 || len(latencies) != 16 {
		t.Error("reliable TCP did not complete exact fixed payload")
	}
	if result.UDPInvalid != 0 || result.UDPDuplicates != 0 || result.UDPReceiveErrors != 0 || result.TargetWriteErrors != 0 {
		t.Error("UDP correctness failure")
	}
	if profile == "loss_reorder_mixed" && (result.UDPOfferedDatagrams != 3600 || result.UDPReceivedDatagrams == 0) {
		t.Error("UDP offered denominator incomplete")
	}
	if result.Up.SchedulerOverflow+result.Down.SchedulerOverflow != 0 {
		t.Error("simulator bounded scheduler overflow")
	}
}
