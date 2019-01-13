// Copyright 2017 Ole Krüger.
// Licensed under the MIT license which can be found in the LICENSE file.

package knxnet

import (
	"golang.org/x/net/ipv4"
	"net"
	"time"

	"github.com/koenijn/knx-go/knx/util"
)

// A Socket is a socket, duh.
type Socket interface {
	Send(payload ServicePackable) error
	Inbound() <-chan Service
	Close() error
}

// TunnelSocket is a UDP socket for KNXnet/IP packet exchange.
type TunnelSocket struct {
	conn    *net.UDPConn
	inbound <-chan Service
}

// DialTunnel creates a new Socket which can used to exchange KNXnet/IP packets with a single
// endpoint.
func DialTunnel(address string) (*TunnelSocket, error) {
	addr, err := net.ResolveUDPAddr("udp4", address)
	if err != nil {
		return nil, err
	}

	conn, err := net.DialUDP("udp4", nil, addr)
	if err != nil {
		return nil, err
	}

	conn.SetDeadline(time.Time{})

	inbound := make(chan Service)
	go serveUDPSocket(conn, addr, inbound)

	return &TunnelSocket{conn, inbound}, nil
}

// Send transmits a KNXnet/IP packet.
func (sock *TunnelSocket) Send(payload ServicePackable) error {
	buffer := make([]byte, Size(payload))
	Pack(buffer, payload)

	// Transmission of the buffer contents
	_, err := sock.conn.Write(buffer)
	return err
}

// Inbound provides a channel from which you can retrieve incoming packets.
func (sock *TunnelSocket) Inbound() <-chan Service {
	return sock.inbound
}

// Close shuts the socket down. This will indirectly terminate the associated workers.
func (sock *TunnelSocket) Close() error {
	return sock.conn.Close()
}

// RouterSocket is a UDP socket for KNXnet/IP packet exchange.
type RouterSocket struct {
	conn    *net.UDPConn
	addr    *net.UDPAddr
	inbound <-chan Service
}

// AnyInterface can be provided as an interface name if you don't want to specific which interface
// shall be used.
const AnyInterface string = ""

// ListenRouter creates a new Socket which can be used to exchange KNXnet/IP packets with
// multiple endpoints.
func ListenRouter(interfaceName string, multicastAddr string, multicastLoopback bool) (_ *RouterSocket, err error) {
	var iface *net.Interface

	if interfaceName != AnyInterface {
		iface, err = net.InterfaceByName(interfaceName)
		if err != nil {
			return
		}
	}

	addr, err := net.ResolveUDPAddr("udp4", multicastAddr)
	if err != nil {
		return
	}

	conn, err := net.ListenUDP("udp4", addr)
	if err != nil {
		return
	}
	pc := ipv4.NewPacketConn(conn)
	if err = pc.JoinGroup(iface, addr); err != nil {
		return
	}
	// enable or disable multicast loopback to make it possible to have more listeners on same machine/interface.
	// DO NOT USE FOR BRIDGE AS IT CAUSES A LOOP!
	pc.SetMulticastLoopback(multicastLoopback)

	conn.SetDeadline(time.Time{})

	inbound := make(chan Service)
	go serveUDPSocket(conn, nil, inbound)

	return &RouterSocket{conn, addr, inbound}, nil
}

// Send transmits a KNXnet/IP packet.
func (sock *RouterSocket) Send(payload ServicePackable) error {
	buffer := make([]byte, Size(payload))
	Pack(buffer, payload)

	// Transmission of the buffer contents
	_, err := sock.conn.WriteToUDP(buffer, sock.addr)
	return err
}

// Inbound provides a channel from which you can retrieve incoming packets.
func (sock *RouterSocket) Inbound() <-chan Service {
	return sock.inbound
}

// Close shuts the socket down. This will indirectly terminate the associated workers.
func (sock *RouterSocket) Close() error {
	return sock.conn.Close()
}

// serveUDPSocket is the receiver worker for a UDP socket.
func serveUDPSocket(conn *net.UDPConn, addr *net.UDPAddr, inbound chan<- Service) {
	util.Log(conn, "Started worker")
	defer util.Log(conn, "Worker exited")

	// A closed inbound channel indicates to its readers that the worker has terminated.
	defer close(inbound)

	buffer := [1024]byte{}

	for {
		len, sender, err := conn.ReadFromUDP(buffer[:])
		if err != nil {
			util.Log(conn, "Error during ReadFromUDP: %v", err)
			return
		}

		// Validate sender origin if necessary
		if addr != nil && (!addr.IP.Equal(sender.IP) || addr.Port != sender.Port) {
			util.Log(conn, "Origin validation failed: %v != %v", addr, sender)
			continue
		}

		var payload Service
		_, err = Unpack(buffer[:len], &payload)
		if err != nil {
			util.Log(conn, "Error during Unpack: %v", err)
			continue
		}

		inbound <- payload
	}
}
