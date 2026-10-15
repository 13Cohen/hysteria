package outbounds

import (
	"net"
	"net/netip"
	"sync"
	"time"
)

const (
	// This is a local reuse limit, not the DNS answer's TTL: net.LookupIP does
	// not expose record TTLs. Hits never extend it. Keep the experiment opt-in.
	udpResolveReuseLifetime   = 10 * time.Second
	udpResolveReuseMaxEntries = 32
)

type udpResolveKey struct {
	host string
	port uint16
}

type udpResolveEntry struct {
	info    *ResolveInfo
	expires time.Time
}

// udpResolveCache belongs to one direct UDP socket. It never shares decisions
// across flows or holds its lock during DNS resolution or a socket write.
// Concurrent misses may resolve independently; no goroutine, timer, global
// singleflight group, or waiting queue is added to the packet path.
type udpResolveCache struct {
	mu      sync.Mutex
	entries map[udpResolveKey]*udpResolveEntry
	closed  bool
	lookup  func(*AddrEx)
	now     func() time.Time
}

func newUDPResolveCache(lookup func(*AddrEx)) *udpResolveCache {
	return &udpResolveCache{
		entries: make(map[udpResolveKey]*udpResolveEntry),
		lookup:  lookup,
		now:     time.Now,
	}
}

func cloneUDPResolveInfo(info *ResolveInfo) *ResolveInfo {
	return &ResolveInfo{
		IPv4: append(net.IP(nil), info.IPv4...),
		IPv6: append(net.IP(nil), info.IPv6...),
		Err:  info.Err,
	}
}

// isUDPLiteralHost includes IPv6 scope zones. Leave their existing direct
// outbound parsing/sending semantics untouched rather than caching as names.
func isUDPLiteralHost(host string) bool {
	_, err := netip.ParseAddr(host)
	return err == nil
}

func (c *udpResolveCache) resolve(addr *AddrEx) (*AddrEx, *udpResolveEntry, error) {
	// Never mutate the caller's address or publish an internal IP slice.
	resolved := &AddrEx{Host: addr.Host, Port: addr.Port}
	if tryParseIP(resolved) {
		return resolved, nil, nil
	}
	key := udpResolveKey{host: addr.Host, port: addr.Port}
	now := c.now()
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, nil, net.ErrClosed
	}
	if entry, ok := c.entries[key]; ok {
		if now.Before(entry.expires) {
			resolved.ResolveInfo = cloneUDPResolveInfo(entry.info)
			c.mu.Unlock()
			return resolved, entry, nil
		}
		delete(c.entries, key)
	}
	c.mu.Unlock()

	c.lookup(resolved)
	info := resolved.ResolveInfo
	if info == nil || info.Err != nil || (info.IPv4 == nil && info.IPv6 == nil) {
		// No negative or stale-if-error cache. Let the normal outbound preserve
		// its existing error handling, including partially successful results.
		return resolved, nil, nil
	}
	now = c.now()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, nil, net.ErrClosed
	}
	// A concurrent lookup may have already published a fresh result. An older,
	// slower lookup must not overwrite it or extend its original expiry.
	if entry, ok := c.entries[key]; ok && now.Before(entry.expires) {
		return resolved, nil, nil
	}
	for k, entry := range c.entries {
		if !now.Before(entry.expires) {
			delete(c.entries, k)
		}
	}
	if len(c.entries) >= udpResolveReuseMaxEntries {
		var oldestKey udpResolveKey
		var oldestExpiry time.Time
		for k, entry := range c.entries {
			if oldestExpiry.IsZero() || entry.expires.Before(oldestExpiry) {
				oldestKey, oldestExpiry = k, entry.expires
			}
		}
		delete(c.entries, oldestKey)
	}
	entry := &udpResolveEntry{
		info:    cloneUDPResolveInfo(info),
		expires: now.Add(udpResolveReuseLifetime),
	}
	c.entries[key] = entry
	return resolved, entry, nil
}

func (c *udpResolveCache) forget(addr *AddrEx, expected *udpResolveEntry) {
	if expected == nil {
		return
	}
	key := udpResolveKey{host: addr.Host, port: addr.Port}
	c.mu.Lock()
	if c.entries[key] == expected {
		delete(c.entries, key)
	}
	c.mu.Unlock()
}

func (c *udpResolveCache) close() {
	c.mu.Lock()
	c.closed = true
	c.entries = nil
	c.mu.Unlock()
}
