#!/usr/bin/env python3
"""
ulak E2E fixture: an MQTT 3.1.1 "broker" that refuses every connection.

Reads one CONNECT packet per client and answers with
CONNACK 0x20 0x02 0x00 <return code>, then waits for the client to close.

Usage: fake-mqtt-broker.py <port> <connack return code>
  3 = server unavailable (transient), 5 = not authorized (permanent)
"""

import socket
import sys
import threading


def read_exact(conn, n):
    data = b""
    while len(data) < n:
        chunk = conn.recv(n - len(data))
        if not chunk:
            raise ConnectionError("client closed")
        data += chunk
    return data


def handle(conn, return_code):
    try:
        conn.settimeout(10)
        header = read_exact(conn, 1)
        if header[0] >> 4 != 1:  # not CONNECT
            return
        length, multiplier = 0, 1
        while True:
            byte = read_exact(conn, 1)[0]
            length += (byte & 0x7F) * multiplier
            multiplier *= 128
            if not byte & 0x80:
                break
        read_exact(conn, length)
        conn.sendall(bytes([0x20, 0x02, 0x00, return_code]))
        conn.recv(1)
    except (OSError, ConnectionError):
        pass
    finally:
        conn.close()


def main():
    port, return_code = int(sys.argv[1]), int(sys.argv[2])
    server = socket.socket()
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("0.0.0.0", port))
    server.listen(16)
    while True:
        conn, _ = server.accept()
        threading.Thread(target=handle, args=(conn, return_code), daemon=True).start()


if __name__ == "__main__":
    main()
