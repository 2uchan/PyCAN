""" Created on 2023
@author: Yuchan Lee, Sookwang Lee, Jaehwan Lee (Korea Aerospace Univ)
"""


import socket
import struct
import random
import pickle
import selectors
import subprocess
import argparse
from requests import get
import time
from datetime import datetime, timedelta
import hashlib
import copy
import os
# import python file
from Zone import Zone
from Neighbor import Neighbor
from NodeBase import NodeBase

if __name__ == '__main__':

    this_ip = socket.gethostbyname(socket.gethostname())
    parser = argparse.ArgumentParser()
    parser.add_argument("--port","-p", help="this peer's port number", type=int, default=random.randint(13001, 40000))
    parser.add_argument("--bootstrap","-B", help="is Bootstrap?" , type=bool, default=False)
    parser.add_argument("--node_num","-N", help="number of node" , type=int, default=1)
    args = parser.parse_args()
    
    this_addr = (this_ip, args.port)

    node = NodeBase(args.port, args.node_num, args.bootstrap) 
    node.run()
    print('bye bye',args.port)
