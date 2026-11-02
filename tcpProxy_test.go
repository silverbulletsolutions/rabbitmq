package rabbitmq

import (
	"net"
	"sync"
	"testing"
)

// tcpProxy forwards TCP connections to target and simulates network / broker failures
type tcpProxy struct {
	ln     net.Listener
	target string

	mu     sync.Mutex
	conns  map[net.Conn]struct{}
	down   bool // new connections are closed immediately (broker unreachable)
	frozen bool // forwarded data is discarded (dead link - no FIN / RST)
}

func newTCPProxy(t *testing.T, target string) *tcpProxy {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	p := &tcpProxy{ln: ln, target: target, conns: make(map[net.Conn]struct{})}
	go p.acceptLoop()
	t.Cleanup(func() {
		_ = p.ln.Close()
		p.Drop()
	})
	return p
}

func (p *tcpProxy) Addr() string {
	return p.ln.Addr().String()
}

// Drop closes all active connections (broker restart / network failure)
func (p *tcpProxy) Drop() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for c := range p.conns {
		_ = c.Close()
	}
	clear(p.conns)
}

// SetDown makes broker unreachable (true) or reachable again (false)
func (p *tcpProxy) SetDown(down bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.down = down
}

// SetFrozen discards all forwarded data in both directions (true) or forwards it again (false)
func (p *tcpProxy) SetFrozen(frozen bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.frozen = frozen
}

func (p *tcpProxy) isFrozen() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.frozen
}

func (p *tcpProxy) acceptLoop() {
	for {
		client, err := p.ln.Accept()
		if err != nil {
			return
		}

		p.mu.Lock()
		down := p.down
		p.mu.Unlock()
		if down {
			_ = client.Close()
			continue
		}

		server, err := net.Dial("tcp", p.target)
		if err != nil {
			_ = client.Close()
			continue
		}

		p.mu.Lock()
		p.conns[client] = struct{}{}
		p.conns[server] = struct{}{}
		p.mu.Unlock()

		go p.pipe(server, client)
		go p.pipe(client, server)
	}
}

func (p *tcpProxy) pipe(dst, src net.Conn) {
	buf := make([]byte, 32*1024)
	for {
		n, err := src.Read(buf)
		if n > 0 && !p.isFrozen() {
			if _, err := dst.Write(buf[:n]); err != nil {
				break
			}
		}
		if err != nil {
			break
		}
	}
	_ = src.Close()
	_ = dst.Close()
}
