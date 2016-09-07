package transport

import (
	"github.com/stefankopieczek/gossip/base"
	"github.com/stefankopieczek/gossip/log"
	"github.com/stefankopieczek/gossip/parser"
)

import "net"

type Tcp struct {
	connTable
	listeningPoints []*net.TCPListener
	parser          *parser.Parser
	output          chan base.SipMessage
	stop            bool
	notifier_       *notifier
}

func NewTcp(output chan base.SipMessage) (*Tcp, error) {
	tcp := Tcp{output: output}
	tcp.listeningPoints = make([]*net.TCPListener, 0)
	tcp.connTable.Init()
	return &tcp, nil
}

func NewTcpWithNotifier() (*Tcp, error) {

	var n notifier
	n.init()

	newTcp, err := NewTcp(n.inputs)
	newTcp.notifier_ = &n

	if err != nil {
		n.stop()
		return nil, err
	}

	return newTcp, nil
}

func (tcp *Tcp) notifier() *notifier {
	return tcp.notifier_
}

func (tcp *Tcp) Listen(address string) error {
	var err error = nil
	addr, err := net.ResolveTCPAddr("tcp", address)
	if err != nil {
		return err
	}

	lp, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return err
	}

	tcp.listeningPoints = append(tcp.listeningPoints, lp)
	go tcp.serve(lp)

	// At this point, err should be nil but let's be defensive.
	return err
}

func (tcp *Tcp) IsStreamed() bool {
	return true
}

func (tcp *Tcp) getConnection(addr string) (*connection, error) {
	conn := tcp.connTable.GetConn(addr)

	if conn == nil {
		log.Debug("No stored connection for address %s; generate a new one", addr)
		raddr, err := net.ResolveTCPAddr("tcp", addr)
		if err != nil {
			return nil, err
		}

		baseConn, err := net.DialTCP("tcp", nil, raddr)
		if err != nil {
			return nil, err
		}

		conn = NewConn(baseConn, tcp.output)
	} else {
		conn = tcp.connTable.GetConn(addr)
	}

	tcp.connTable.Notify(addr, conn)
	return conn, nil
}

func (tcp *Tcp) Send(addr string, msg base.SipMessage) error {
	conn, err := tcp.getConnection(addr)
	if err != nil {
		return err
	}

	err = conn.Send(msg)
	return err
}

func (tcp *Tcp) serve(listeningPoint *net.TCPListener) {
	log.Info("Begin serving TCP on address " + listeningPoint.Addr().String())

	for {
		baseConn, err := listeningPoint.Accept()
		if err != nil {
			log.Severe("Failed to accept TCP conn on address " + listeningPoint.Addr().String() + "; " + err.Error())
			continue
		}

		conn := NewConn(baseConn, tcp.output)
		log.Debug("Accepted new TCP conn %p from %s on address %s", &conn, conn.baseConn.RemoteAddr(), conn.baseConn.LocalAddr())
		tcp.connTable.Notify(baseConn.RemoteAddr().String(), conn)
	}
}

func (tcp *Tcp) Stop() {
	tcp.connTable.Stop()
	tcp.stop = true
	for _, lp := range tcp.listeningPoints {
		lp.Close()
	}
}
