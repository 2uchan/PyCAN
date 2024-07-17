""" Created on 2023
@author: Yuchan Lee, Sookwang Lee, Jaehwan Lee (Korea Aerospace Univ)
"""

import numpy as np
#import torch
import threading
import math
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

from Zone import Zone

class Neighbor:
    def __init__(self, address, point, zone, s_stack, data_point):
        self.addr = address
        self.neighbor_table = dict()

        self.neighbor_table[self.addr] = [point, zone, s_stack,data_point]

    def get_hash_zone(self):
        return self.point

    def get_address(self):
        return self.addr

    def get_zone(self, address):
        return self.neighbor_table[address][0:1]

    def get_neighbor_table(self):
        return self.neighbor_table
    
    def get_nn_table(self):
        return self.neighbor_table.keys()
    
    def neighbor_update(self, zone):
        node_zone = Zone(*list(sum(zone,[])))
        remove_addr = []
        for a, [p,z,s,dp] in self.neighbor_table.items():
            n_z = Zone(*list(sum(z,[])))
            if node_zone.isNeighbor(n_z) == False:
                remove_addr.append(a)
        for k in remove_addr:
            del(self.neighbor_table[k])
        return self.neighbor_table
    
