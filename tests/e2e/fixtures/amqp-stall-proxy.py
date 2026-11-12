#!/usr/bin/env python3
"""
ulak E2E fixture: TCP proxy in front of an AMQP 0-9-1 broker that goes silent.

Relays both directions until the client sends a channel.open method frame
(class 20, method 10); from then on nothing from the broker reaches the
client, so the client waits for channel.open-ok that never arrives.

Usage: amqp-stall-proxy.py <listen port> <broker host> <broker port>
"""

import socket
import sys
import threading

PROTOCOL_HEADER_LEN = 8  # "AMQP" 0 0 9 1
FRAME_METHOD = 1
CHANNEL_CLASS, CHANNEL_OPEN = 20, 10


def client_to_broker(client, broker, state):
    buf = b""
    header_seen = False
    try:
        while True:
            data = client.recv(65536)
            if not data:
                break
            broker.sendall(data)
            buf += data
            if not header_seen:
                if len(buf) < PROTOCOL_HEADER_LEN:
                    continue
                buf = buf[PROTOCOL_HEADER_LEN:]
                header_seen = True
            # frame: type(1) channel(2) size(4) payload(size) end(1)
            while len(buf) >= 7:
                size = int.from_bytes(buf[3:7], "big")
                if len(buf) < 7 + size + 1:
                    break
                payload = buf[7:7 + size]
                if (buf[0] == FRAME_METHOD and len(payload) >= 4
                        and int.from_bytes(payload[0:2], "big") == CHANNEL_CLASS
                        and int.from_bytes(payload[2:4], "big") == CHANNEL_OPEN):
                    state["stalled"] = True
                buf = buf[7 + size + 1:]
    except OSError:
        pass
    finally:
        for sock in (client, broker):
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass


def broker_to_client(client, broker, state):
    try:
        while True:
            data = broker.recv(65536)
            if not data:
                break
            if not state["stalled"]:
                client.sendall(data)
    except OSError:
        pass


def handle(client, host, port):
    broker = socket.create_connection((host, port))
    state = {"stalled": False}
    threading.Thread(target=client_to_broker, args=(client, broker, state), daemon=True).start()
    broker_to_client(client, broker, state)
    client.close()
    broker.close()


def main():
    listen_port, host, port = int(sys.argv[1]), sys.argv[2], int(sys.argv[3])
    server = socket.socket()
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("0.0.0.0", listen_port))
    server.listen(16)
    while True:
        client, _ = server.accept()
        threading.Thread(target=handle, args=(client, host, port), daemon=True).start()


if __name__ == "__main__":
    main()
