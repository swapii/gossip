package transport

import (
	"github.com/stefankopieczek/gossip/base"
	"github.com/stefankopieczek/gossip/log"
	"github.com/stefankopieczek/gossip/parser"
)

import (
	"net"
)

type UdpSingle struct {
	connection *net.UDPConn
	output     chan base.SipMessage
	stop       bool
	notifier_  *notifier
}

func NewUdpSingle(output chan base.SipMessage) (*UdpSingle, error) {

	newUdp := UdpSingle{output: output}

	return &newUdp, nil
}

func NewUdpSingleWithNotifier() (*UdpSingle, error) {

	var n notifier
	n.init()

	newUdp, err := NewUdpSingle(n.inputs)
	newUdp.notifier_ = &n

	if err != nil {
		n.stop()
		return nil, err
	}

	return newUdp, nil
}

func (udp *UdpSingle) notifier() *notifier {
	return udp.notifier_
}

func (udp *UdpSingle) Listen(address string) error {

	addr, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		return err
	}

	conn, err := net.ListenUDP("udp", addr)

	if err == nil {

		if udp.connection != nil {
			// close previous connection
			e := udp.connection.Close()
			if e != nil {
				return e
			}
		}

		udp.connection = conn
		go udp.listen(conn)
	}

	return err
}

func (udp *UdpSingle) IsStreamed() bool {
	return false
}

func (udp *UdpSingle) Send(addr string, msg base.SipMessage) error {
	log.Debug("Sending message %s to %s", msg.Short(), addr)
	raddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return err
	}

	_, err = udp.connection.WriteTo([]byte(msg.String()), raddr)

	return err
}

func (udp *UdpSingle) listen(conn *net.UDPConn) {
	log.Info("Begin listening for UDP on address %s", conn.LocalAddr())

	buffer := make([]byte, c_BUFSIZE)
	for {
		num, _, err := conn.ReadFromUDP(buffer)
		if err != nil {
			if udp.stop {
				log.Info("Stopped listening for UDP on %s", conn.LocalAddr)
				break
			} else {
				log.Severe("Failed to read from UDP buffer: " + err.Error())
				continue
			}
		}

		pkt := append([]byte(nil), buffer[:num]...)
		go func() {
			msg, err := parser.ParseMessage(pkt)
			if err != nil {
				log.Warn("Failed to parse SIP message: %s", err.Error())
			} else {
				udp.output <- msg
			}
		}()
	}
}

func (udp *UdpSingle) Stop() {
	udp.stop = true
	udp.connection.Close()
}
