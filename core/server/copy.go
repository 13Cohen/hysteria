package server

import (
	"errors"
	"io"
	"time"
)

var errDisconnect = errors.New("traffic logger requested disconnect")

func copyBufferLog(dst io.Writer, src io.Reader, log func(n uint64) bool) error {
	return copyBufferLogObserved(dst, src, log, nil)
}

func copyBufferLogObserved(dst io.Writer, src io.Reader, log func(n uint64) bool, evidence *streamDirection) (result error) {
	if evidence != nil {
		defer func() { evidence.finish(result) }()
	}
	buf := make([]byte, 32*1024)
	for {
		if evidence != nil {
			evidence.pending.Store(0)
			evidence.begin(streamPhaseRead)
		}
		nr, er := src.Read(buf)
		if nr > 0 {
			var accountingStarted time.Time
			if evidence != nil {
				evidence.read(nr)
				accountingStarted = evidence.begin(streamPhaseAccounting)
			}
			allowed := log(uint64(nr))
			if evidence != nil {
				var accountingError error
				if !allowed {
					accountingError = errDisconnect
				}
				evidence.completeOperation("accounting", accountingStarted, nr, 0, accountingError)
			}
			if !allowed {
				// Log returns false, which means that the client should be disconnected
				return errDisconnect
			}
			var started time.Time
			if evidence != nil {
				started = evidence.begin(streamPhaseWrite)
			}
			nw, ew := dst.Write(buf[0:nr])
			if evidence != nil {
				evidence.wrote(nw, nr, started, ew)
			}
			if ew != nil {
				return ew
			}
		}
		if er != nil {
			if er == io.EOF {
				// EOF should not be considered as an error
				return nil
			}
			return er
		}
	}
}

func copyTwoWayEx(id string, serverRw, remoteRw io.ReadWriter, l TrafficLogger, stats *StreamStats, evidence *evidenceStream) error {
	var toClient, toTarget *streamDirection
	if evidence != nil {
		toClient, toTarget = &evidence.toClient, &evidence.toTarget
	}
	errChan := make(chan error, 2)
	go func() {
		errChan <- copyBufferLogObserved(serverRw, remoteRw, func(n uint64) bool {
			stats.LastActiveTime.Store(time.Now())
			stats.Rx.Add(n)
			return l.LogTraffic(id, 0, n)
		}, toClient)
	}()
	go func() {
		errChan <- copyBufferLogObserved(remoteRw, serverRw, func(n uint64) bool {
			stats.LastActiveTime.Store(time.Now())
			stats.Tx.Add(n)
			return l.LogTraffic(id, n, 0)
		}, toTarget)
	}()
	// Block until one of the two goroutines returns
	return <-errChan
}

// copyTwoWay is the "fast-path" version of copyTwoWayEx that does not log traffic or update stream stats.
// It uses the built-in io.Copy instead of our own copyBufferLog.
func copyTwoWay(serverRw, remoteRw io.ReadWriter) error {
	errChan := make(chan error, 2)
	go func() {
		_, err := io.Copy(serverRw, remoteRw)
		errChan <- err
	}()
	go func() {
		_, err := io.Copy(remoteRw, serverRw)
		errChan <- err
	}()
	// Block until one of the two goroutines returns
	return <-errChan
}
