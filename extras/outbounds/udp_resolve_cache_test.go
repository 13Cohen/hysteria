package outbounds

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func testUDPResolve(t *testing.T, c *udpResolveCache, addr *AddrEx) (*AddrEx, *udpResolveEntry) {
	t.Helper()
	resolved, entry, err := c.resolve(addr)
	if err != nil {
		t.Fatal(err)
	}
	return resolved, entry
}

func TestUDPResolveReuseLifetimeAndOwnership(t *testing.T) {
	var calls int
	var tick atomic.Int64
	c := newUDPResolveCache(func(a *AddrEx) {
		calls++
		a.ResolveInfo = &ResolveInfo{IPv4: net.IPv4(127, 0, 0, byte(calls)).To4(), IPv6: net.ParseIP("::1")}
	})
	c.now = func() time.Time { return time.Unix(0, tick.Load()) }
	addr := &AddrEx{Host: "reuse.test", Port: 443}
	first, _ := testUDPResolve(t, c, addr)
	first.ResolveInfo.IPv4[3] = 99
	first.ResolveInfo.IPv6[15] = 99
	second, _ := testUDPResolve(t, c, addr)
	if calls != 1 || !second.ResolveInfo.IPv4.Equal(net.IPv4(127, 0, 0, 1)) || !second.ResolveInfo.IPv6.Equal(net.ParseIP("::1")) {
		t.Fatal("a cache result shares mutable IP slices or was not reused")
	}
	second.ResolveInfo.IPv4[3] = 98
	if addr.ResolveInfo != nil {
		t.Fatal("caller address was mutated")
	}
	tick.Store(int64(9 * time.Second))
	testUDPResolve(t, c, addr)
	if calls != 1 {
		t.Fatal("expired before the local lifetime")
	}
	tick.Store(int64(udpResolveReuseLifetime))
	fresh, _ := testUDPResolve(t, c, addr)
	if calls != 2 || !fresh.ResolveInfo.IPv4.Equal(net.IPv4(127, 0, 0, 2)) {
		t.Fatal("hit extended expiry or stale result survived expiry")
	}
}

func TestUDPResolveReuseTargetsAndBound(t *testing.T) {
	var calls int
	var tick int64
	c := newUDPResolveCache(func(a *AddrEx) {
		calls++
		a.ResolveInfo = &ResolveInfo{IPv4: net.IPv4(127, 0, 0, 1)}
	})
	c.now = func() time.Time { tick++; return time.Unix(0, tick) }
	first := &AddrEx{Host: "reuse.test", Port: 1}
	testUDPResolve(t, c, first)
	testUDPResolve(t, c, &AddrEx{Host: "other.test", Port: 1})
	testUDPResolve(t, c, &AddrEx{Host: "reuse.test", Port: 2})
	if calls != 3 {
		t.Fatal("different host or port incorrectly shared a result")
	}
	for i := 0; i < udpResolveReuseMaxEntries; i++ {
		testUDPResolve(t, c, &AddrEx{Host: fmt.Sprintf("target-%d.test", i), Port: 443})
	}
	if len(c.entries) != udpResolveReuseMaxEntries {
		t.Fatalf("entry bound: %d", len(c.entries))
	}
	before := calls
	testUDPResolve(t, c, first)
	if calls != before+1 || len(c.entries) != udpResolveReuseMaxEntries {
		t.Fatal("oldest entry was not evicted within the cap")
	}
}

func TestUDPResolveReuseFailuresAreNotCached(t *testing.T) {
	failure := errors.New("synthetic lookup failure")
	for _, name := range []string{"error", "empty", "partial-error"} {
		t.Run(name, func(t *testing.T) {
			var calls int
			var tick int64
			c := newUDPResolveCache(func(a *AddrEx) {
				calls++
				a.ResolveInfo = &ResolveInfo{}
				if calls == 1 || calls >= 4 {
					a.ResolveInfo.IPv4 = net.IPv4(127, 0, 0, byte(calls))
				} else {
					if name != "empty" {
						a.ResolveInfo.Err = failure
					}
					if name == "partial-error" {
						a.ResolveInfo.IPv4 = net.IPv4(127, 0, 0, 9)
					}
				}
			})
			c.now = func() time.Time { return time.Unix(0, tick) }
			addr := &AddrEx{Host: "failure.test", Port: 443}
			testUDPResolve(t, c, addr)
			tick = int64(udpResolveReuseLifetime)
			for i := 0; i < 2; i++ {
				result, entry := testUDPResolve(t, c, addr)
				if entry != nil || len(c.entries) != 0 {
					t.Fatal("failure was cached or stale entry retained")
				}
				if name != "empty" && !errors.Is(result.ResolveInfo.Err, failure) {
					t.Fatal("failure semantics changed")
				}
				if result.ResolveInfo.IPv4.Equal(net.IPv4(127, 0, 0, 1)) {
					t.Fatal("stale-if-error")
				}
			}
			testUDPResolve(t, c, addr)
			testUDPResolve(t, c, addr)
			if calls != 4 {
				t.Fatalf("failure recovery calls=%d", calls)
			}
		})
	}
}

func TestUDPResolveReuseFailedWriteDoesNotEvictReplacement(t *testing.T) {
	var calls int
	var tick int64
	c := newUDPResolveCache(func(a *AddrEx) { calls++; a.ResolveInfo = &ResolveInfo{IPv4: net.IPv4(127, 0, 0, byte(calls))} })
	c.now = func() time.Time { return time.Unix(0, tick) }
	addr := &AddrEx{Host: "reuse.test", Port: 443}
	_, oldEntry := testUDPResolve(t, c, addr)
	tick = int64(udpResolveReuseLifetime)
	_, newEntry := testUDPResolve(t, c, addr)
	c.forget(addr, oldEntry)
	testUDPResolve(t, c, addr)
	if calls != 2 {
		t.Fatal("late failure evicted newer result")
	}
	c.forget(addr, newEntry)
	testUDPResolve(t, c, addr)
	if calls != 3 {
		t.Fatal("matching failed result was not evicted")
	}
}

func TestUDPResolveReuseLookupDoesNotLockHitsOrClose(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	c := newUDPResolveCache(func(a *AddrEx) {
		if a.Host == "slow.test" {
			close(entered)
			<-release
		}
		a.ResolveInfo = &ResolveInfo{IPv4: net.IPv4(127, 0, 0, 1)}
	})
	testUDPResolve(t, c, &AddrEx{Host: "fast.test", Port: 443})
	done := make(chan error, 1)
	go func() { _, _, err := c.resolve(&AddrEx{Host: "slow.test", Port: 443}); done <- err }()
	<-entered
	operations := make(chan error, 1)
	go func() {
		_, _, err := c.resolve(&AddrEx{Host: "fast.test", Port: 443})
		c.close()
		operations <- err
	}()
	select {
	case err := <-operations:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		close(release)
		t.Fatal("lookup held the map lock across DNS")
	}
	close(release)
	if err := <-done; !errors.Is(err, net.ErrClosed) {
		t.Fatalf("in-flight completion after close: %v", err)
	}
	if len(c.entries) != 0 {
		t.Fatal("closed cache refilled")
	}
}

func TestUDPResolveReuseConcurrentAccess(t *testing.T) {
	var calls atomic.Int64
	c := newUDPResolveCache(func(a *AddrEx) { calls.Add(1); a.ResolveInfo = &ResolveInfo{IPv4: net.IPv4(127, 0, 0, 1)} })
	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < 40; i++ {
				addr := &AddrEx{Host: fmt.Sprintf("concurrent-%d.test", i%7), Port: uint16(4000 + worker)}
				resolved, entry, err := c.resolve(addr)
				if err != nil {
					t.Error(err)
					return
				}
				resolved.ResolveInfo.IPv4[0] = 99
				if i%3 == 0 {
					c.forget(addr, entry)
				}
			}
		}(worker)
	}
	wg.Wait()
	if len(c.entries) > udpResolveReuseMaxEntries || calls.Load() == 0 {
		t.Fatal("cache bound or lookup count")
	}
	c.close()
	if _, _, err := c.resolve(&AddrEx{Host: "closed.test", Port: 1}); !errors.Is(err, net.ErrClosed) {
		t.Fatal("closed cache accepted a name")
	}
}

func TestUDPResolveLiteralClassification(t *testing.T) {
	for _, host := range []string{"127.0.0.1", "::1", "fe80::1%lo", "::ffff:127.0.0.1"} {
		if !isUDPLiteralHost(host) {
			t.Fatalf("literal classified as DNS name: %s", host)
		}
	}
	if isUDPLiteralHost("name.test") {
		t.Fatal("name classified as literal")
	}
}
