from __future__ import print_function, unicode_literals
import socket
import json
from async import WeakMethod, TcpListener, TcpReconnector


class JlpPeer(object):
    def __init__(self, message_handler):
        self.socket = None
        self.incoming_buffer = ""
        self.message_handler = message_handler
        
        
    def set_socket(self, socket):
        self.socket = socket
        self.metapoll.register_reader(socket, WeakMethod(self.recved))
        
        
    def send_message(self, params):
        self.socket.send(json.dumps(params) + "\n")
        
        
    def recved(self):
        while True:
            recved = None

            try:
                recved = self.socket.recv(65536)
            except socket.error as e:
                if e.errno == errno.EAGAIN:
                    break

                print("Socket error while receiving: %s" % e)
                self.metapoll.register_reader(self.socket, None)
                self.socket = None
                break

            if not recved:
                self.metapoll.register_reader(self.socket, None)
                self.socket = None
                break

            self.incoming_buffer += recved

        messages = self.incoming_buffer.split("\n")
        n = len(messages) - 1
        
        for i in range(n):
            self.message_handler(json.loads(messages[i]))
            
        self.incoming_buffer = messages[-1]
    

class JlpClient(JlpPeer):
    def __init__(self, metapoll, server_addr, message_handler):
        super(JlpClient, self).__init__(message_handler)
        
        self.reconnector = TcpReconnector(metapoll, server_addr, timeout, WeakMethod(self.connected))


    def connected(self, socket):
        self.set_socket(socket)
    

class JlpServer(JlpPeer):
    def __init__(self, metapoll, server_addr, message_handler):
        super(JlpServer, self).__init__(message_handler)

        self.listener = TcpListener(metapoll, server_addr, WeakMethod(self.accepted))


    def accepted(self, socket, id):
        self.set_socket(socket)
