package outbounds

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/miekg/dns"
)

// This test deliberately uses real Go DNS requests and real UDP payloads, all
// confined to loopback. No Internet resolver or destination is contacted.
func TestUDPResolveReuseLoopbackDNS(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("reuse=%t", enabled), func(t *testing.T) {
			var queries, aQueries, aaaaQueries atomic.Int64
			pc, err := net.ListenPacket("udp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			server := &dns.Server{PacketConn: pc, Handler: dns.HandlerFunc(func(w dns.ResponseWriter, req *dns.Msg) {
				queries.Add(1)
				time.Sleep(5 * time.Millisecond)
				resp := new(dns.Msg)
				resp.SetReply(req)
				for _, q := range req.Question {
					switch q.Qtype {
					case dns.TypeA:
						aQueries.Add(1)
						resp.Answer = append(resp.Answer, &dns.A{Hdr: dns.RR_Header{Name: q.Name, Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: 1}, A: net.IPv4(127, 0, 0, 1)})
					case dns.TypeAAAA:
						aaaaQueries.Add(1)
						resp.Answer = append(resp.Answer, &dns.AAAA{Hdr: dns.RR_Header{Name: q.Name, Rrtype: dns.TypeAAAA, Class: dns.ClassINET, Ttl: 1}, AAAA: net.ParseIP("::1")})
					}
				}
				_ = w.WriteMsg(resp)
			})}
			ready, serverDone := make(chan struct{}), make(chan error, 1)
			server.NotifyStartedFunc = func() { close(ready) }
			go func() { serverDone <- server.ActivateAndServe() }()
			select {
			case <-ready:
			case <-time.After(time.Second):
				t.Fatal("DNS fixture did not start")
			}
			t.Cleanup(func() {
				_ = server.Shutdown()
				if err := <-serverDone; err != nil {
					t.Error(err)
				}
			})
			previous := net.DefaultResolver
			net.DefaultResolver = &net.Resolver{PreferGo: true, Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, network, pc.LocalAddr().String())
			}}
			t.Cleanup(func() { net.DefaultResolver = previous })

			receiver, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
			if err != nil {
				t.Fatal(err)
			}
			defer receiver.Close()
			opts := DirectOutboundOptions{Mode: DirectOutboundMode4, UDPResolveReuse: enabled}
			ob, err := NewDirectOutboundWithOptions(opts)
			if err != nil {
				t.Fatal(err)
			}
			adapter := &PluggableOutboundAdapter{ob}
			target := net.JoinHostPort("udp-reuse-fixture.test", strconv.Itoa(receiver.LocalAddr().(*net.UDPAddr).Port))
			conn, err := adapter.UDP(target)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			const packets = 16
			start := time.Now()
			for i := 0; i < packets; i++ {
				payload := append([]byte(fmt.Sprintf("packet-%02d:", i)), bytes.Repeat([]byte{byte(i), 0xff, 0x00}, 128)...)
				n, err := conn.WriteTo(payload, target)
				if err != nil || n != len(payload) {
					t.Fatalf("write %d n=%d err=%v", i, n, err)
				}
				_ = receiver.SetReadDeadline(time.Now().Add(2 * time.Second))
				actual := make([]byte, len(payload)+1)
				n, _, err = receiver.ReadFromUDP(actual)
				if err != nil || !bytes.Equal(actual[:n], payload) {
					t.Fatalf("payload %d mismatch n=%d err=%v", i, n, err)
				}
			}
			duration := time.Since(start)
			want := int64(packets)
			if enabled {
				want = 1
			}
			if aQueries.Load() != want || aaaaQueries.Load() != want || queries.Load() != want*2 {
				t.Fatalf("queries=%d A=%d AAAA=%d; want %d pairs", queries.Load(), aQueries.Load(), aaaaQueries.Load(), want)
			}
			t.Logf("reuse=%t packets=%d intact=%d DNS_queries=%d A=%d AAAA=%d fixture_DNS_delay_ms=5 elapsed_ms=%.3f", enabled, packets, packets, queries.Load(), aQueries.Load(), aaaaQueries.Load(), float64(duration.Microseconds())/1000)
		})
	}
}

func TestUDPResolveReusePreservesSuppliedInfoAndLiterals(t *testing.T) {
	receiver, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer receiver.Close()
	ob, err := NewDirectOutboundWithOptions(DirectOutboundOptions{Mode: DirectOutboundMode4, UDPResolveReuse: true})
	if err != nil {
		t.Fatal(err)
	}
	conn, err := ob.UDP(&AddrEx{Host: "ignored.test", Port: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	direct := conn.(*directOutboundUDPConn)
	direct.resolveCache.lookup = func(a *AddrEx) { t.Error("cache consulted for supplied resolution or literal") }
	port := uint16(receiver.LocalAddr().(*net.UDPAddr).Port)
	for _, addr := range []*AddrEx{
		{Host: "supplied.test", Port: port, ResolveInfo: &ResolveInfo{IPv4: net.IPv4(127, 0, 0, 1), IPv6: net.ParseIP("::1")}},
		{Host: "127.0.0.1", Port: port},
	} {
		payload := []byte("preserve-address-semantics")
		n, err := conn.WriteTo(payload, addr)
		if err != nil || n != len(payload) {
			t.Fatal(n, err)
		}
		_ = receiver.SetReadDeadline(time.Now().Add(time.Second))
		actual := make([]byte, 128)
		n, _, err = receiver.ReadFromUDP(actual)
		if err != nil || !bytes.Equal(actual[:n], payload) {
			t.Fatal("payload mismatch", err)
		}
	}
	if len(direct.resolveCache.entries) != 0 {
		t.Fatal("supplied info or literal entered DNS cache")
	}
}

func TestUDPResolveReuseAddressFamilyModes(t *testing.T) {
	for _, test := range []struct {
		name  string
		mode  DirectOutboundMode
		want6 bool
		bound bool
	}{
		{"auto", DirectOutboundModeAuto, false, false}, {"4", DirectOutboundMode4, false, false}, {"46", DirectOutboundMode46, false, false},
		{"6", DirectOutboundMode6, true, false}, {"64", DirectOutboundMode64, true, false},
		{"bound4", DirectOutboundMode4, false, true}, {"bound6", DirectOutboundMode6, true, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			network, ip := "udp4", net.IPv4(127, 0, 0, 1)
			if test.want6 {
				network, ip = "udp6", net.ParseIP("::1")
			}
			receiver, err := net.ListenUDP(network, &net.UDPAddr{IP: ip})
			if err != nil {
				if test.want6 {
					t.Skip("IPv6 loopback unavailable")
				}
				t.Fatal(err)
			}
			defer receiver.Close()
			opts := DirectOutboundOptions{Mode: test.mode, UDPResolveReuse: true}
			if test.bound {
				if test.want6 {
					opts.BindIP6 = ip
				} else {
					opts.BindIP4 = ip
				}
			}
			ob, err := NewDirectOutboundWithOptions(opts)
			if err != nil {
				t.Fatal(err)
			}
			conn, err := ob.UDP(&AddrEx{Host: "mode.test", Port: 1, ResolveInfo: &ResolveInfo{IPv4: net.IPv4(127, 0, 0, 1), IPv6: net.ParseIP("::1")}})
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			conn.(*directOutboundUDPConn).resolveCache.lookup = func(a *AddrEx) { a.ResolveInfo = &ResolveInfo{IPv4: net.IPv4(127, 0, 0, 1), IPv6: net.ParseIP("::1")} }
			addr := &AddrEx{Host: "mode.test", Port: uint16(receiver.LocalAddr().(*net.UDPAddr).Port)}
			for i := 0; i < 2; i++ {
				payload := []byte("family-selection")
				if _, err := conn.WriteTo(payload, addr); err != nil {
					t.Fatal(err)
				}
				_ = receiver.SetReadDeadline(time.Now().Add(time.Second))
				actual := make([]byte, 128)
				n, _, err := receiver.ReadFromUDP(actual)
				if err != nil || !bytes.Equal(actual[:n], payload) {
					t.Fatal("mode changed destination family", err)
				}
			}
		})
	}
}

func TestUDPResolveReuseWriteFailureAndRecovery(t *testing.T) {
	receiver, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer receiver.Close()
	ob, err := NewDirectOutboundWithOptions(DirectOutboundOptions{Mode: DirectOutboundMode4, UDPResolveReuse: true})
	if err != nil {
		t.Fatal(err)
	}
	conn, err := ob.UDP(&AddrEx{Host: "write-failure.test", Port: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	direct := conn.(*directOutboundUDPConn)
	var calls int
	direct.resolveCache.lookup = func(a *AddrEx) { calls++; a.ResolveInfo = &ResolveInfo{IPv4: net.IPv4(127, 0, 0, 1)} }
	addr := &AddrEx{Host: "write-failure.test", Port: uint16(receiver.LocalAddr().(*net.UDPAddr).Port)}
	payload := []byte("send-failure-recovery")
	writeAndRead := func() {
		t.Helper()
		if _, err := conn.WriteTo(payload, addr); err != nil {
			t.Fatal(err)
		}
		_ = receiver.SetReadDeadline(time.Now().Add(time.Second))
		actual := make([]byte, 128)
		n, _, err := receiver.ReadFromUDP(actual)
		if err != nil || !bytes.Equal(actual[:n], payload) {
			t.Fatal("payload mismatch", err)
		}
	}
	writeAndRead()
	_ = direct.UDPConn.SetWriteDeadline(time.Now().Add(-time.Second))
	if _, err := conn.WriteTo(payload, addr); err == nil {
		t.Fatal("expired socket write deadline did not fail")
	}
	if len(direct.resolveCache.entries) != 0 || calls != 1 {
		t.Fatal("failed send did not evict its cached result")
	}
	_ = direct.UDPConn.SetWriteDeadline(time.Time{})
	writeAndRead()
	if calls != 2 {
		t.Fatal("send recovery did not resolve again")
	}
}
