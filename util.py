import socket


def resolve(addr):
    return (socket.gethostbyname(addr[0]), addr[1])
