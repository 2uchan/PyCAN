import argparse
import socket
import pickle
import json
import time

if __name__ == '__main__':
    this_ip = socket.gethostbyname(socket.gethostname())
    parser = argparse.ArgumentParser()
    parser.add_argument("--removed_port","-p", nargs='*', type = int, default=1000)
    parser.add_argument("--range_start","-rs", help="first port of range", type=int, default=12000)
    parser.add_argument("--range_end","-re", help="last port of range", type=int, default=100)
    parser.add_argument("--address","-A", help="help peer's ip address", type=str, default=this_ip)
    args = parser.parse_args()
    ip=args.address
    port= args.range_start
    ex_port = args.removed_port if isinstance(args.removed_port, (list, tuple)) else [args.removed_port]
    this_addr = (this_ip,12000)
    while (port <= args.range_end):
        if port in ex_port:
            port+=1
            continue
        node_addr = (ip, port)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(node_addr)
        data = ('node scan', node_addr, this_addr)
        sock.send(pickle.dumps(data))
        sock.close()
        port += 1
