package integration_tests

import (
	"bytes"
	"encoding/json"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/apernet/hysteria/core/v2/client"
	"github.com/apernet/hysteria/core/v2/internal/integration_tests/mocks"
	"github.com/apernet/hysteria/core/v2/server"
	"github.com/stretchr/testify/mock"
)

type evidenceCapture struct{ events chan server.QUICEvidence }

func (c *evidenceCapture) LogQUICEvidence(e server.QUICEvidence) { c.events <- e }

func TestQUICEvidenceAuthenticatedTransfer(t *testing.T) {
	udpConn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	auth := mocks.NewMockAuthenticator(t)
	auth.EXPECT().Authenticate(mock.Anything, "private-bad-password", uint64(0)).Return(false, "").Once()
	auth.EXPECT().Authenticate(mock.Anything, "private-good-password", uint64(0)).Return(true, "private-auth-identity").Once()
	capture := &evidenceCapture{events: make(chan server.QUICEvidence, 64)}
	s, err := server.NewServer(&server.Config{TLSConfig: serverTLSConfig(), Conn: udpConn,
		Authenticator: auth, QUICEvidenceConfig: server.QUICEvidenceConfig{Enabled: true, Interval: time.Second}, QUICEvidenceLogger: capture})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	go s.Serve()
	newClient := func(password string) (client.Client, error) {
		c, _, err := client.NewClient(&client.Config{ServerAddr: udpConn.LocalAddr(), Auth: password,
			// Matches the upstream fixture tests; this is not certificate-policy validation.
			TLSConfig: client.TLSConfig{InsecureSkipVerify: true}})
		return c, err
	}
	bad, err := newClient("private-bad-password")
	if err == nil || bad != nil {
		if bad != nil {
			bad.Close()
		}
		t.Fatal("invalid credentials were accepted")
	}
	select {
	case e := <-capture.events:
		t.Fatalf("unauthenticated event leaked: %+v", e)
	default:
	}
	c, err := newClient("private-good-password")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	echo, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	echoServer := &tcpEchoServer{Listener: echo}
	defer echoServer.Close()
	go echoServer.Serve()
	conn, err := c.TCP(echo.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("private-payload"), 2048)
	if _, err := conn.Write(payload); err != nil {
		t.Fatal(err)
	}
	response := make([]byte, len(payload))
	if _, err := io.ReadFull(conn, response); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(payload, response) {
		t.Fatal("tracing changed payload transfer")
	}
	var session string
	var observed bool
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	for !observed {
		select {
		case e := <-capture.events:
			data, err := json.Marshal(e)
			if err != nil {
				t.Fatal(err)
			}
			for _, secret := range []string{"private-payload", "private-good-password", "private-bad-password", "private-auth-identity", echo.Addr().String(), udpConn.LocalAddr().String()} {
				if strings.Contains(string(data), secret) {
					t.Fatalf("private field reached evidence: %s", secret)
				}
			}
			if session == "" {
				session = e.Session
			} else if e.Session != session {
				t.Fatal("one connection changed session ID")
			}
			observed = e.Event == "sample" && e.ReceivedPackets > 0 && e.SentPackets > 0 && e.AcknowledgedPackets > 0 && e.LastMetricsAgeMS >= 0 && e.LatestRTTUS > 0 && e.CongestionWindow > 0
		case <-timer.C:
			t.Fatal("real QUIC traffic did not produce authenticated ACK/RTT/window evidence")
		}
	}
	conn.Close()
	c.Close()
	for {
		select {
		case e := <-capture.events:
			if e.Event == "closed" {
				if e.Session != session {
					t.Fatal("close changed session ID")
				}
				return
			}
		case <-timer.C:
			t.Fatal("authenticated close evidence missing")
		}
	}
}
