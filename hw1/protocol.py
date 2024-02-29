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


BUF_SIZE = 4096
RAW_BUF_SIZE = 1 + sizeof(u64) + BUF_SIZE
ACK_ID = -1
ACK_PACKET = 0
DATA_PACKET = 1


def get_int(data):
    return int.from_bytes(data[:sizeof(u64)], byteorder='little')


class MyTCPProtocol(UDPBasedProtocol):
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.recv_mutex = threading.Lock()
        self.recv_cond = threading.Condition(self.recv_mutex)
        self.recv_buf = bytearray()
        self.recv_n = 0
        self.recv_fin = False
        self.recv_ack = []
        self.recv_pending = {}
        self.recv_id = 0
        self.recv_thread = threading.Thread(target=self._recv, daemon=True)

        self.send_mutex = threading.Lock()
        self.send_cond = threading.Condition(self.send_mutex)
        self.send_pending = {}
        self.send_queue = [] # heap
        self.send_id = 0
        self.send_thread = threading.Thread(target=self._send, daemon=True)

        self.recv_thread.start()
        self.send_thread.start()

    def _recv(self):
        while True:
            ##print(f'_recv$$$ {self.recvfrom(30)} {self.remote_addr}')
            ##print(f'_recv$$$ {self.recvfrom(30)} {self.remote_addr}')
            ##print(f'_recv$$$ {self.recvfrom(30)} {self.remote_addr}')
            packet = self.recvfrom(RAW_BUF_SIZE)


            if len(packet) == 0:
                self.recv_cond.acquire()
                self.recv_fin = True
                self.recv_cond.notify()
                self.recv_cond.release()
                return

            packet_type = packet[0]

            # assuming packet format is correct
            if packet_type == DATA_PACKET:
                packet_id = get_int(packet[1:])
                msg = packet[1 + sizeof(u64):]

                #print(f'_recv {packet_id} {msg_len} {msg[:11]}')

                self.send_cond.acquire()
                self.recv_ack.append(packet_id)
                if ACK_ID not in self.send_pending:
                    t = time.clock_gettime_ns(time.CLOCK_MONOTONIC)
                    heapq.heappush(self.send_queue, (t + 1*5_000_000, ACK_ID)) # send ACK after 5ms
                    self.send_pending[ACK_ID] = None
                    self.send_cond.notify()
                self.send_cond.release()

                if self.recv_id <= packet_id:
                    self.recv_pending[packet_id] = msg

                self.recv_cond.acquire()
                while self.recv_id in self.recv_pending:
                    self.recv_buf += self.recv_pending[self.recv_id]
                    self.recv_pending.pop(self.recv_id)
                    self.recv_id += 1

                if len(self.recv_buf) >= self.recv_n:
                    self.recv_cond.notify()
                self.recv_cond.release()
            elif packet_type == ACK_PACKET:
                msg = packet[1:]

                self.send_cond.acquire()
                for i in range(0, len(msg), sizeof(u64)):
                    packet_id = get_int(msg[i:])
                    if packet_id in self.send_pending:
                        self.send_pending.pop(packet_id)
                self.send_cond.release()

    def _send(self):
        while True:
            self.send_cond.acquire()
            while not self.send_pending:
                self.send_cond.wait()

            t = time.clock_gettime_ns(time.CLOCK_MONOTONIC)
            if self.send_queue and self.send_queue[0][0] < t:
                packet_id = self.send_queue[0][1]
                if packet_id in self.send_pending:
                    if packet_id != ACK_ID:
                        self.sendto(self.send_pending[packet_id])

                        #print(f'_send----DATA {self.send_pending[packet_id]}')
                        heapq.heapreplace(self.send_queue, (t + 50_000_000, packet_id)) # resend after 50ms if no ACK received
                    else:
                        packet = bytes([ACK_PACKET]) + bytes().join((bytes(u64(i)) for i in self.recv_ack))
                        self.sendto(packet)
                        #print(f'_send----ACK {packet}')
                        self.recv_ack = []
                        self.send_pending.pop(ACK_ID)
                        heapq.heappop(self.send_queue)
                else:
                    heapq.heappop(self.send_queue)
                t = time.clock_gettime_ns(time.CLOCK_MONOTONIC)
            
            self.send_cond.release()

    def recv(self, n: int):
        #print(f'recv>>>>>> {self.remote_addr}')
        self.recv_cond.acquire()
        #print(f'recv+++++++ {self.remote_addr} {len(self.recv_buf)}')
        self.recv_n = n
        while len(self.recv_buf) < n and not self.recv_fin:
            self.recv_cond.wait()

        #print(f'recv******* {self.remote_addr}')
        if self.recv_fin:
            self.recv_thread.join()

        n = min(n, len(self.recv_buf))
        msg = self.recv_buf[:n]
        self.recv_buf = self.recv_buf[n:]

        self.recv_cond.release()
        #print(f'recv<<<<<<< {self.remote_addr}')
        #print(msg)
        return msg

    def send(self, data: bytes):
        #print(f'send>>>>>> {self.remote_addr}')
        sent = 0
        n = len(data)
        while sent < n:
            self.send_cond.acquire()
            data_size = BUF_SIZE if sent + BUF_SIZE <= n else n - sent
            packet_id = self.send_id
            packet = bytes([DATA_PACKET]) + bytes(u64(packet_id)) + data[sent : sent + data_size]
            #print(f'send#### {packet}')
            self.send_pending[packet_id] = packet
            t = time.clock_gettime_ns(time.CLOCK_MONOTONIC)
            heapq.heappush(self.send_queue, (t, packet_id))
            self.send_id += 1
            sent += data_size
            self.send_cond.notify()
            self.send_cond.release()
        #print(f'send<<<<<<< {self.remote_addr}')
        return n

