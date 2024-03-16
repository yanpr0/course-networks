from ctypes import c_uint64 as u64, sizeof
import heapq
import socket
import threading
import time


class UDPBasedProtocol:
    def __init__(self, *, local_addr, remote_addr):
        self.udp_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
        self.remote_addr = remote_addr
        self.udp_socket.bind(local_addr)

    def sendto(self, data):
        return self.udp_socket.sendto(data, self.remote_addr)

    def recvfrom(self, n):
        msg, addr = self.udp_socket.recvfrom(n)
        return msg


BUF_SIZE = 4096 * 8
RAW_BUF_SIZE = 1 + sizeof(u64) + BUF_SIZE
ACK_PACKET = 0
DATA_PACKET = 1
ACK_DELAY = 100_000_000


def get_int(data):
    return int.from_bytes(data[:sizeof(u64)], byteorder='little')


class MyTCPProtocol(UDPBasedProtocol):
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.recv_mutex = threading.Lock()
        self.recv_cond = threading.Condition(self.recv_mutex)
        self.recv_buf = bytearray()
        self.recv_ack = []
        self.recv_pending = []
        self.recv_id = 0
        self.recv_thread = threading.Thread(target=self._recv, daemon=True)

        self.send_mutex = threading.Lock()
        self.send_cond = threading.Condition(self.send_mutex)
        self.send_pending = {}
        self.send_queue = [] # heap
        self.send_id = 0
        self.send_thread = threading.Thread(target=self._send, daemon=True)

        self.ack_mutex = threading.Lock()
        self.ack_thread = None

        self.recv_thread.start()
        self.send_thread.start()

    def _ack(self):
        with self.ack_mutex:
            packet = bytes([ACK_PACKET]) + bytes().join((bytes(u64(i)) for i in self.recv_ack))
            self.recv_ack.clear()
            self.ack_thread = None
        self.sendto(packet)

    def _recv(self):
        while True:
            packet = self.recvfrom(RAW_BUF_SIZE)

            packet_type = packet[0]

            # assuming packet format is correct
            if packet_type == DATA_PACKET:
                packet_id = get_int(packet[1:])
                msg = packet[1 + sizeof(u64):]

                with self.ack_mutex:
                    if self.ack_thread is None:
                        self.ack_thread = threading.Timer(0.02, self._ack)
                        self.ack_thread.start()
                    self.recv_ack.append(packet_id)

                if self.recv_id <= packet_id:
                    self.recv_pending.append((packet_id, msg))

                curr_id = self.recv_id
                self.recv_pending.sort(reverse=True)
                with self.recv_cond:
                    while self.recv_pending and curr_id == self.recv_pending[-1][0]:
                        msg = self.recv_pending.pop(-1)[1]
                        self.recv_buf += msg
                        curr_id += 1
                    self.recv_cond.notify()
                self.recv_id = curr_id
            elif packet_type == ACK_PACKET:
                with self.send_cond:
                    for i in range(1, len(packet), sizeof(u64)):
                        packet_id = get_int(packet[i:])
                        if packet_id in self.send_pending:
                            self.send_pending.pop(packet_id)

    def _send(self):
        while True:
            self.send_cond.acquire()
            while not self.send_pending:
                self.send_cond.wait()

            t = time.clock_gettime_ns(time.CLOCK_MONOTONIC)
            while self.send_queue and self.send_queue[0][0] < t:
                packet_id = self.send_queue[0][1]
                if packet_id not in self.send_pending:
                    heapq.heappop(self.send_queue)
                else:
                    self.sendto(self.send_pending[packet_id])
                    heapq.heapreplace(self.send_queue, (t + ACK_DELAY, packet_id)) # resend after timeout if no ACK received

                t = time.clock_gettime_ns(time.CLOCK_MONOTONIC)
            
            self.send_cond.release()

    def recv(self, n: int):
        with self.recv_cond:
            while len(self.recv_buf) < n:
                self.recv_cond.wait()

            msg = self.recv_buf[:n]
            self.recv_buf = self.recv_buf[n:]

        return msg

    def send(self, data: bytes):
        sent = 0
        n = len(data)
        self.send_cond.acquire()
        while sent < n:
            data_size = BUF_SIZE if sent + BUF_SIZE <= n else n - sent
            packet_id = self.send_id
            packet = bytes().join((bytes([DATA_PACKET]), bytes(u64(packet_id)), data[sent : sent + data_size]))
            self.sendto(packet)

            self.send_pending[packet_id] = packet
            t = time.clock_gettime_ns(time.CLOCK_MONOTONIC)
            heapq.heappush(self.send_queue, (t + ACK_DELAY, packet_id))

            self.send_id += 1
            sent += data_size

        self.send_cond.notify()
        self.send_cond.release()

        return n

