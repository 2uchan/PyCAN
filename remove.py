import argparse
import socket
import pickle

this_ip = socket.gethostbyname(socket.gethostname())
parser = argparse.ArgumentParser()
parser.add_argument("--port", "-p", help="this peer's port number", type=int, default=12007)
parser.add_argument("--address","-A", help="help peer's ip address", type=str, default=this_ip)
args = parser.parse_args()

this_addr = (args.address, args.port)
print(this_addr)
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.connect(this_addr)

data = ('leave',00)
sock.sendall(pickle.dumps(data))

sock.close()