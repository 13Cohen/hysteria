package cmd

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apernet/hysteria/core/v2/server"
	"github.com/miekg/dns"
	"github.com/spf13/viper"
)

// Exercise the public YAML -> outbound pipeline -> real DNS -> real UDP path;
// a test of the parsed bool alone would miss a broken options mapping.
func TestServerUDPResolveReuseConfig(t *testing.T) {
	var queries atomic.Int64
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ready := make(chan struct{})
	ds := &dns.Server{PacketConn: pc, NotifyStartedFunc: func() { close(ready) }, Handler: dns.HandlerFunc(func(w dns.ResponseWriter, r *dns.Msg) {
		queries.Add(1)
		m := new(dns.Msg)
		m.SetReply(r)
		for _, q := range r.Question {
			if q.Qtype == dns.TypeA {
				m.Answer = append(m.Answer, &dns.A{Hdr: dns.RR_Header{Name: q.Name, Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: 1}, A: net.IPv4(127, 0, 0, 1)})
			} else if q.Qtype == dns.TypeAAAA {
				m.Answer = append(m.Answer, &dns.AAAA{Hdr: dns.RR_Header{Name: q.Name, Rrtype: dns.TypeAAAA, Class: dns.ClassINET, Ttl: 1}, AAAA: net.ParseIP("::1")})
			}
		}
		_ = w.WriteMsg(m)
	})}
	done := make(chan error, 1)
	go func() { done <- ds.ActivateAndServe() }()
	<-ready
	defer func() {
		_ = ds.Shutdown()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	previous := net.DefaultResolver
	net.DefaultResolver = &net.Resolver{PreferGo: true, Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, pc.LocalAddr().String())
	}}
	defer func() { net.DefaultResolver = previous }()
	target, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	_, port, _ := net.SplitHostPort(target.LocalAddr().String())
	addr := net.JoinHostPort("yaml-udp-reuse.test", port)
	for _, mode := range []string{"omitted", "false", "true"} {
		t.Run(mode, func(t *testing.T) {
			yaml := "outbounds:\n  - name: default\n    type: direct\n    direct:\n      mode: '4'\n"
			if mode != "omitted" {
				yaml += fmt.Sprintf("      udpResolveReuse: %s\n", mode)
			}
			v := viper.New()
			v.SetConfigType("yaml")
			if err := v.ReadConfig(strings.NewReader(yaml)); err != nil {
				t.Fatal(err)
			}
			var cfg serverConfig
			if err := v.Unmarshal(&cfg); err != nil {
				t.Fatal(err)
			}
			var core server.Config
			if err := cfg.fillOutboundConfig(&core); err != nil {
				t.Fatal(err)
			}
			conn, err := core.Outbound.UDP(addr)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			before := queries.Load()
			for i := 0; i < 2; i++ {
				payload := []byte{0x5a, byte(i)}
				n, err := conn.WriteTo(payload, addr)
				if err != nil || n != len(payload) {
					t.Fatal(n, err)
				}
				_ = target.SetReadDeadline(time.Now().Add(time.Second))
				buf := make([]byte, 8)
				n, _, err = target.ReadFrom(buf)
				if err != nil || !bytes.Equal(payload, buf[:n]) {
					t.Fatal("UDP mismatch", err)
				}
			}
			want := int64(4)
			if mode == "true" {
				want = 2
			}
			if got := queries.Load() - before; got != want {
				t.Fatalf("YAML %s queries=%d want=%d", mode, got, want)
			}
		})
	}
}
