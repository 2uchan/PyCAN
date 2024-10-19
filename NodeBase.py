""" Created on 2023bor
@author: Yuchan Lee, Sookwang Lee, Jaehwan Lee (Korea Aerospace Univ)
"""

import numpy as np
import time
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
import sys
import binascii
import json

from Zone import Zone
from Neighbor import Neighbor
from Split import Split

class NodeBase:
    def __init__(self, port, node_num, bootstrap):
        with open('./setting.json') as f:
            config = json.load(f)
        self.this_ip = socket.gethostbyname(socket.gethostname())
        self.this_port = port
        self.this_addr = (self.this_ip, self.this_port)
        self.host_addr = (config['host_addr'], config['host_port'])
        self.dimension = config['dimension']
        self.node_num = node_num
        self.bootstrap = bootstrap
        self.max_zone = config['max_zone']
        self.point = self.hash_func(self.this_addr)
        self.ctime = int(time.time())
        self.heart_time= self.ctime + 10
        self.timer_time = self.ctime + random.randint(50,150)
        self.nn_table = dict()
        self.timer = dict()
        self.datapoint_dict = dict()
        self.data_dict = dict()
        self.file_list = list()
        self.alone = True
        self.check =0
        self.zlock = threading.Lock()
        self.slock = threading.Lock()
        self.tlock = threading.Lock()
        self.nlock = threading.Lock()
        self.nnlock = threading.Lock()
        self.dlock = threading.Lock()
        self.dplock = threading.Lock()
        self.szlock = threading.Lock()  ## 측정용
        self.size = dict()  ##측정용 나중에 지우기

        
    def run(self):
        #self.point = self.hash_to_zone(args.hash_text, self.dimension, self.max_zone, 123)
        if self.bootstrap:
            self.alone = False
            self.zone_list = []
            for i in range(self.dimension):
                self.zone_list.append(0)
                self.zone_list.append(self.max_zone)
            self.z = Zone(*self.zone_list)  # Initialized Bootstrap node
            self.n = Neighbor(self.this_addr, self.point, self.z.zone,None,None)  # Neighbout table setting (this_address, point, zone)
            self.s = Split(None, None)
            self.s.history = [sublist for sublist in self.s.history if sublist != [None, None]]
            print('Bootstrap hash table :',self.this_addr,self.point) # Print initialized bootstrap point
        else:
            #for i in range(args.dimension):
            #    self.point[i] = random.randint(0,self.max_zone)
            self.port = self.this_port
            self.z = None
            self.client_table = dict()
            self.sucess = False
            
            #print("node IP is:", this_ip)
            #print("node Port is:", self.port)
            print('Node hash table :',self.this_addr,self.point) # Print initialized node point

        # socket setting
        self.alive = True

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(self.this_addr)
        self.socket.setblocking(False)
        self.socket.listen(5)

        # set listening daemon
        self.selector = selectors.DefaultSelector()
        self.selector.register(self.socket, selectors.EVENT_READ, self.accept_handler)
        self.listen_t = threading.Thread(target=self.listener, daemon=True, name="listener")
        self.listen_t.start()
        
        if self.bootstrap == False :
            self.join()
        while self.alive:
            time.sleep(2)
            if not self.alone:
                self.stablize()
        
    def stablize(self):
        self.ctime = int(time.time())
        if self.ctime>=self.heart_time: 
            with self.zlock:
                with self.slock:
                    with self.nlock:
                        sending_data = (self.point,self.z.zone,self.s.history,self.n.neighbor_table.items(),self.datapoint_dict)
                        sending_data = str(sending_data)
                        hash_data = hashlib.md5(sending_data.encode()).hexdigest()
                        msg = ('heart beat', self.this_addr, hash_data)
                        for a in self.n.neighbor_table.keys():
                            try:
                                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                                sock.connect(a)
                                sock.sendall(pickle.dumps(msg))
                                sock.close()
                                self.heart_time = self.ctime + 30
                            except Exception as e:
                                print(f"An error occured: {e}",'heart beat','from',self.this_addr,'to',a)
                                self.heart_time = self.ctime + random.randint(2,7)
                        self.n.neighbor_update(self.z.zone)
            
        '''
        if self.ctime >=self.check:
            n_zone = []
            with self.nlock:
                for a,[p,z,s,d] in self.n.neighbor_table.items():
                    n_zone.append(z)
            with self.zlock:
                if self.z.mini_eureka(n_zone,self.max_zone):
                    self.check=self.ctime + 300
                else:
                    if self.ctime == self.heart_time:
                        self.check = self.ctime+25
                    else:
                        self.check = self.ctime+20
                    msg = ('neighbor list request', self.this_addr)
                    with self.nlock:
                        for a in self.n.neighbor_table.keys():
                            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                            try:
                                sock.connect(a)
                                sock.sendall(pickle.dumps(msg))
                                sock.close()
                            except Exception as e:
                                print(f"An error occured: {e}",msg[0],'from',self.this_addr,'to',a)
        '''
        if self.ctime >= self.timer_time: 
            remove_nn =[]
            with self.nnlock:
                for na,nn in self.nn_table.items():     ##neighbor가 아닌 노드 nntable에서 삭제
                    if na not in self.n.neighbor_table.keys():
                        remove_nn.append(na)
                for na in remove_nn:
                    del self.nn_table[na]
            expired_list = []
            with self.tlock:
                for ta,t in self.timer.items():     ##이웃이 아닌 노드 타이머에서 삭제
                    with self.nlock:
                        if ta not in self.n.neighbor_table:
                            expired_list.append(ta)
                        else:
                            if self.ctime-t >=100:
                                msg = ('info request', self.this_addr)
                                self.sender(msg,ta)
                                self.n.neighbor_update(self.z.zone)

                            elif self.ctime-t >=150:
                                print(self.this_addr,ta,'timer expired')
                                print('-'*10,"terminate",ta,'-'*10)
                                msg = ('terminate',ta)
                                if ta in self.nn_table:
                                    with self.nnlock:
                                        for a, [p,z,s,dp] in self.nn_table[ta].items():
                                            try:
                                                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                                                sock.connect(a)
                                                sock.sendall(pickle.dumps(msg))
                                                sock.close()
                                            except Exception as e:
                                                print(f"An error occured: {e}",msg[0],'from',self.this_addr,'to',a)
                                terminate_addr = ta
                                with self.tlock:
                                    if terminate_addr in self.timer.keys():
                                        del(self.timer[terminate_addr])
                                with self.zlock:
                                    with self.slock:
                                        with self.nlock:
                                            if terminate_addr in self.n.neighbor_table.keys():
                                                [tp,tz,ts,tdp] = self.n.neighbor_table[terminate_addr]
                                                t_zone = Zone(*list(sum(tz,[])))
                                                t_history = Split(None,None)
                                                t_history.history = t_history.history + ts
                                                valid, expand, poped_history=t_history.valid(self.z.zone,t_zone.zone)
                                                if valid:
                                                    self.z = Zone(*list(sum(expand, [])))
                                                    print('-'*10,self.this_addr, 'original Zone expanded to merge',terminate_addr,'-'*10)
                                                    #self.z.show()
                                                    #print('history:',self.s.get_Split_history())
                                                    self.s.erase(poped_history)             ## 사용된 분기정보가 나의 스택에서도 있었다면 삭제
                                                    with self.nnlock:
                                                        if terminate_addr in self.nn_table:
                                                            terminated_neighbor_table = self.nn_table.pop(terminate_addr)
                                                            for a, [p, z, s, dp] in terminated_neighbor_table.items():
                                                                self.n.neighbor_table[a] = [p, z, s, dp]
                                                    self.n.neighbor_update(self.z.zone)
                                                    with self.dplock:
                                                        for dn, dd in tdp.items():
                                                            self.datapoint_dict[dn]=dd
                                                        self.datapoint_update_zdp()
                                                    if terminate_addr in self.n.neighbor_table:
                                                        del(self.n.neighbor_table[terminate_addr])    ## nieghbor에서 삭제
                                                    with self.dplock:
                                                        self.n.neighbor_table[self.this_addr] = [self.point, self.z.zone, self.s.get_Split_history(),self.datapoint_dict]
                                                    self.check = int(time.time())+300
                                                    msg = ('neighbor update', self.this_addr, self.n.get_neighbor_table())
                                                    for a in self.n.neighbor_table.keys():
                                                        self.sender(msg,a)
                                                    if self.this_addr in self.n.neighbor_table:
                                                        del(self.n.neighbor_table[self.this_addr])
                                                else:
                                                    del(self.n.neighbor_table[terminate_addr])
                                self.check=self.ctime + 300
                                                    
                for ta in expired_list:
                    if ta in self.timer.keys():
                        del self.timer[ta]
                with self.nlock:
                    for ta in self.n.neighbor_table.keys():    # 타이머에 없는 이웃 추가
                        if ta not in self.timer:
                            self.timer[ta]=self.ctime     
                self.timer_time = self.ctime + 100
                
    def join(self):
        msg = ('join', self.this_addr, self.point)
        self.sender(msg,self.host_addr)

    def listener(self):
        while self.alive:
            self.selector = selectors.DefaultSelector()
            self.selector.register(self.socket, selectors.EVENT_READ, self.accept_handler)
            while self.alive:
                for (key, mask) in self.selector.select():
                    key: selectors.SelectorKey
                    srv_sock, callback = key.fileobj, key.data
                    callback(srv_sock, self.selector)

    def accept_handler(self, sock: socket.socket, sel: selectors.BaseSelector):
        """
        accept connection from other nodes
        """
        conn: socket.socket
        conn, addr = self.socket.accept()
        sel.register(conn, selectors.EVENT_READ, self.read_handler)

        
    
    def read_handler(self, conn: socket.socket, sel: selectors.BaseSelector):
        """
        read msg from other nodes
        """
        message = "---- wait for recv[any other] from {}".format(conn.getpeername())
        recv_data = b""
        while True:
            data_chunk = conn.recv(1024)
            if not data_chunk:  # The loop ends when message reception is complete
                break
            recv_data += data_chunk  # Accumulate received msg
            
        received_data = recv_data
        msg =  (received_data)
        recv_time = int(time.time())      ## 측정용
        with self.szlock:
            if recv_time in self.size.keys():
                self.size[recv_time] += sys.getsizeof(msg)
            else:
                self.size[recv_time] = sys.getsizeof(msg)
            
        msg = pickle.loads(received_data)
        threading.Thread(target=self._handle, args=((msg,conn)), daemon=True).start()
        sel.unregister(conn)
    
    def hash_func(self,msg):
        org_data = str(msg)
        hash_data = int(hashlib.md5(org_data.encode()).hexdigest(), 16)
        zone_list = []
        while ((hash_data//self.max_zone**self.dimension)<0):
            hash_data*(self.max_zone+self.dimension)
        for i in range(self.dimension):
            zone_list.append(hash_data%self.max_zone)
            hash_data //= self.max_zone
        return tuple(zone_list)
    
    def routing_zn(self,hm,past_queue):
        if self.z.contain(hm):
            return self.this_addr,None
        else:
            min_distance = self.max_zone ** self.dimension
            past_queue.append(self.this_addr)
            temp_neigh = copy.deepcopy(self.n.neighbor_table)
            for past in past_queue:
                if past in temp_neigh.keys():
                    del(temp_neigh[past])
            for a, [p, z, s, dp] in temp_neigh.items():
                z = Zone(*list(sum(z, [])))
                if z.contain(hm):
                    return a,past_queue
                else:
                    orth,o_dist=z.orthogonal(hm,min_distance)
                    if orth:
                        if o_dist < min_distance:
                            min_distance= o_dist
                            min_addr = a
                    else:
                        dist=z.vertex_dist(hm,min_distance)
                        if dist < min_distance:
                            min_distance= dist
                            min_addr = a
        return min_addr,past_queue
    
    def sender(self,msg,dest):
        try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect(dest)
                sock.sendall(pickle.dumps(msg))
                sock.close()
        except Exception as e:
            print(f"An error occured: {e}",msg[0],'from',self.this_addr,'to',dest)
    
    def data_add(self,data_name,data_content):
        data_point = self.hash_func(data_name)
        empty_queue = []
        with self.dlock:
            self.data_dict[data_point] = data_content
        with self.zlock:
            with self.nlock:
                addr,past_queue = self.routing_zn(data_point,empty_queue)
        if addr == self.this_addr:
            with self.dplock:
                self.datapoint_dict[data_point]= self.this_addr
                print(data_point,'is in',self.this_addr)
                print(self.datapoint_dict.keys())
        else:
            msg = ('data add', self.this_addr,data_point,past_queue)
            self.sender(msg,addr)
           
    def file_add(self,file_name):
        data_point = self.hash_func(file_name)
        empty_queue = []
        with self.zlock:
            with self.nlock:
                addr,past_queue = self.routing_zn(data_point,empty_queue)
        if addr == self.this_addr:
            with self.dplock:
                self.datapoint_dict[data_point]= self.this_addr
                print(data_point,'is in',self.this_addr)
                print(self.datapoint_dict.keys())
        else:
            msg = ('data add', self.this_addr,data_point,past_queue)
            self.sender(msg,addr)
            
    def data_remove(self,data_name):
        data_point = self.hash_func(data_name)
        empty_queue = []
        if data_point in self.data_dict:
            with self.zlock:
                with self.nlock:
                    addr,past_queue = self.routing_zn(data_point,empty_queue)
            if addr == self.this_addr:
                with self.dplock:
                    del(self.datapoint_dict[data_point])
            else:
                msg = ('data remove', self.this_addr, data_name,data_point,past_queue)
                self.sender(msg,addr)
                
    def data_search(self,data_name):
        data_point = self.hash_func(data_name)
        empty_queue = []
        with self.zlock:
            with self.nlock:
                addr,past_queue = self.routing_zn(data_point,empty_queue)
        if addr == self.this_addr:
            with self.dplock:
                if data_name in self.datapoint_dict.keys():
                    print(self.this_addr,"own this data's address")
                    if self.datapoint_dict[data_point] == self.this_addr:
                        print('you already have this data')
                    else:
                        msg = ('data request', self.this_addr, self.this_addr, data_name)
                        self.sender(msg,self.datapoint_dict[data_point])
                else:
                    print("wrong dataname")
                
        else:
            msg = ('data search',self.this_addr, data_name,data_point,past_queue)
            self.sender(msg,addr)
    
    def recieved_data(self,data_name):
        data_point = self.hash_func(data_name)
        with self.dlock:
            return self.data_dict[data_point]
            
    def datapoint_update_zdp(self):     ### require zlock & dplock
        update_list = []
        for data_point in self.datapoint_dict.keys():
            if self.z.contain(data_point) == False:
                update_list.append(data_point)
        for del_point in update_list:
            del(self.datapoint_dict[del_point])
            
    def _handle(self, msg, conn: socket.socket):
        """
        handle msg from other nodes
        """

        if msg[0] == 'join':
            #print("Node", msg[1], "join.")
            # msg : ('join', new Node (ip, port), new Node (point))
            join_node = msg[1]
            join_point = msg[2]
            min_distance = self.max_zone ** self.dimension
            empty_queue = []
            with self.zlock:
                with self.nlock:
                    min_addr,past_queue = self.routing_zn(join_point,empty_queue)

            #self.client_table[join_node] = join_point
            msg = ('zone check',self.this_addr, join_node, join_point, past_queue)
            self.sender(msg,min_addr)  
                            

        elif msg[0] == 'zone check':
            past_addr = msg[1]
            new_node_addr = msg[2]
            new_node_point = msg[3]
            past_queue = msg[4]

            # msg : ('zone check', new Node (ip, port), new Node (point), number of node, queue node)
            #print('receive queue node',this_addr, msg[4])
            #print(queue_nodes)
            with self.zlock:
                if self.z.contain(new_node_point):
                    print('This node', self.this_addr,self.z.zone,'is included in the join point!')
                    origin_zone, join_zone, s_axis, s_cut = self.z.Split_Axis(self.point, new_node_point, self.dimension)
                    self.z = Zone(*list(sum(origin_zone, [])))
                    with self.slock:
                        self.s.history.append([s_axis,s_cut])
                    print('-'*10,self.this_addr, 'original Zone Changed','by',msg[2],'-'*10)
                    self.z.show()
                    # neighbor_update(peer zone, join node (ip, port), join node zone, join node point)
                    #self.n.neighbor_update(self.z.zone, new_node_addr, join_zone, new_node_point)
                    with self.slock:
                        print('history:',self.s.get_Split_history())
                    with self.nlock:
                        with self.dplock:
                            self.n.neighbor_table[self.this_addr] = [self.point, self.z.zone, self.s.get_Split_history(),self.datapoint_dict]
                            msg = ('set zone', self.this_addr, join_zone, self.n.get_neighbor_table(),self.s.history,self.nn_table,self.datapoint_dict)
                            self.sender(msg,new_node_addr) 
                            self.datapoint_update_zdp()   
                        self.n.neighbor_update(self.z.zone)
                        
                    self.sucess = True
                    self.check=self.ctime + 300
                    
                    
                else :
                    #print('This node is not included in the join point!')
                    past_queue.append(past_addr)
                    with self.nlock:
                        temp_neighbor = copy.deepcopy(self.n.get_neighbor_table())

                    for past_node in past_queue:
                            if past_node in temp_neighbor.keys():
                                del(temp_neighbor[past_node])
                    self.sucess = False

                    #del(temp_neighbor[self.this_addr])
                    for a , [p,z,s,dp] in temp_neighbor.items():
                        z = Zone(*list(sum(z, [])))
                        if z.contain(new_node_point):
                            print('The join point',new_node_point, 'is included in the spatial zone of Address', a, z.zone)
                            msg = ('zone check', self.this_addr, new_node_addr, new_node_point,past_queue)
                            self.sender(msg,a)
                                
                            self.sucess = True
                            #self.n.neighbor_table[self.this_addr] = [self.point, self.z.zone]
                            break
                    if self.sucess != True:
                        print('Both this address',self.this_addr,self.z.zone,'and the neighbor node do not include the join point.')
                        min_distance = self.max_zone ** self.dimension
                        for a, [p,z,s,dp], in temp_neighbor.items():
                            z = Zone(*list(sum(z, [])))
                            orth,o_dist=z.orthogonal(new_node_point,min_distance)
                            if orth:
                                if o_dist < min_distance:
                                    min_distance= o_dist
                                    min_addr = a
                            else:
                                dist=z.vertex_dist(new_node_point,min_distance)
                                if dist < min_distance:
                                    min_distance= dist
                                    min_addr = a
                        msg = ('zone check', self.this_addr, new_node_addr, new_node_point, past_queue)
                        self.sender(msg,min_addr)
                        self.sucess = False

        elif msg[0] == 'neighbor update' and not self.alone:

            neigh_addr = msg[1]
            update_neighbor_table = msg[2]
            with self.nnlock:
                self.nn_table[neigh_addr] = update_neighbor_table

            with self.nlock:
                for a, [p, z, s, dp] in update_neighbor_table.items():
                    self.n.neighbor_table[a] = [p, z, s, dp]
        # self.n.neighbor_table = copy.deepcopy(msg[1])
                self.n.neighbor_update(self.z.zone)
                with self.nnlock:
                    self.nn_table[self.this_addr] = self.n.get_neighbor_table()
            remove_nn =[]
            with self.nlock:
                with self.nnlock:
                    for na,nn in self.nn_table.items():     ##neighbor가 아닌 노드 nntable에서 삭제
                        if na not in self.n.neighbor_table.keys():
                            remove_nn.append(na)

                    for na in remove_nn:
                        del self.nn_table[na]
            time.sleep(round(random.uniform(0,1),3))
            with self.nlock:
                msg = ('neighbor neighbor update',self.this_addr,self.n.get_neighbor_table()) 
                for a in self.n.neighbor_table.keys():
                    self.sender(msg,a)
            self.check=self.ctime + 300
                
            #print(self.this_addr,'update!')
            #print(self.this_addr,'neighbor table:', self.n.neighbor_table.keys(), '\n')
        
        elif msg[0] == 'neighbor neighbor update' and not self.alone:

            neigh_addr = msg[1]
            with self.nnlock:
                self.nn_table[neigh_addr] = msg[2]
            remove_nn =[]
            remove_nc =[]
            with self.nlock:
                with self.nnlock:
                    for na,nn in self.nn_table.items():     ##neighbor가 아닌 노드 nntable에서 삭제
                        if na not in self.n.neighbor_table.keys():
                            remove_nn.append(na)
                    for na in remove_nn:
                        del self.nn_table[na]
                

        elif msg[0] == 'set zone':
            # msg : ('set zone', join zone, neighbor_table (Contain neighbor), address (Contain neighbor), zone (Contain neighbor))
            contain_neighbor_addr = msg[1]
            join_zone = msg[2]
            contain_neighbor_table = msg[3]
            s_info = msg[4]
            contain_nn_table = msg[5]
            contain_neighbor_datapoint = msg[6]
            self.check=self.ctime + 300
            with self.zlock:
                self.z = Zone(*list(sum(join_zone, [])))
            print('-'*10 ,self.this_addr, 'Set Zone', '-'*10)
            with self.zlock:
                self.z.show()
            with self.slock:
                self.s = Split(None,None)
                self.s.history = s_info
                print('history:',self.s.get_Split_history())
            contain_neighbor_list = contain_neighbor_table.keys()
            with self.zlock:
                with self.dplock:
                    self.datapoint_dict=contain_neighbor_datapoint
                    self.datapoint_update_zdp()
            with self.zlock:
                with self.slock:
                    contain_neighbor_table[self.this_addr] = [self.point, self.z.zone, self.s.get_Split_history(), self.datapoint_dict]
                    with self.nlock:
                        self.n = Neighbor(self.this_addr, self.point, self.z.zone,self.s.get_Split_history(),self.datapoint_dict)
                        self.n.neighbor_table = copy.deepcopy(contain_neighbor_table)
                        self.n.neighbor_update(self.z.zone)
                        with self.dplock:
                            self.n.neighbor_table[self.this_addr] = [self.point, self.z.zone, self.s.get_Split_history(), self.datapoint_dict]
                        with self.nnlock:
                            self.nn_table = contain_nn_table
                            self.nn_table[self.this_addr] = self.n.get_neighbor_table()
            self.alone = False
            for a in contain_neighbor_list:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                with self.nlock:
                    msg = ('neighbor update', self.this_addr, self.n.get_neighbor_table())
                    self.sender(msg,a)
            #time.sleep(3)

            log_file = open("./log.txt", "w")
            log_file.write("Queue reset!"+"("+str(self.node_num)+")")
            log_file.close()
            print("done!")
            self.check = int(time.time())+300

        elif msg[0] == 'node scan':
            print('make file Node{}'.format(self.node_num))
            f = open("/enter_your_log_directory/Node{}.txt".format(self.node_num),'w')
            f.write(str(self.this_addr)+'\n')
            f.write(str(self.z.getCoords())+'\n')
            f.write(str(self.point)+'\n') 
            f.write(str(self.s.history)+'\n')
            f.write(str(self.datapoint_dict)+'\n')
            f.write(str(self.n.neighbor_table)+'\n')
            f.write(str(self.nn_table)+'\n')
            f.close()
            with self.szlock:
                with open("/home/enter_your_size_directory/Node{}.txt".format(self.node_num),'w') as file:      ## 측정용
                    for key, value in self.size.items():
                        file.write(f"{key} {value}\n")
            '''
            with open("/home/enter_your_size_directory/Node{}.txt".format(self.node_num),'w') as file:      ## 측정용
                size = copy.deepcopy(self.header)
                for key, value in size.items():
                    file.write(f"{key} {value}\n")
            '''
        elif msg[0] == 'leave':
            print('-'*10 ,self.this_addr, 'Leave', '-'*10)
            with self.zlock:
                self.z.show()
            with self.slock:
                print('history:',self.s.get_Split_history())
            with self.nlock:
                msg = ('terminate',self.this_addr)
                for a, [p,z,s,dp] in self.n.get_neighbor_table().items():
                    self.sender(msg,a)
            self.alive=False
            conn.close

        elif msg[0] == 'terminate' and not self.alone:
            terminate_addr = msg[1]
            with self.tlock:
                if terminate_addr in self.timer.keys():
                    del(self.timer[terminate_addr])
            with self.zlock:
                with self.slock:
                    with self.nlock:
                        if terminate_addr in self.n.neighbor_table.keys():
                            [tp,tz,ts,tdp] = self.n.neighbor_table[terminate_addr]
                            t_zone = Zone(*list(sum(tz,[])))
                            t_history = Split(None,None)
                            t_history.history = t_history.history + ts
                            valid, expand, poped_history=t_history.valid(self.z.zone,t_zone.zone)
                            if valid:
                                self.z = Zone(*list(sum(expand, [])))
                                print('-'*10,self.this_addr, 'original Zone expanded to merge',terminate_addr,'-'*10)
                                #self.z.show()
                                #print('history:',self.s.get_Split_history())
                                self.s.erase(poped_history)             ## 사용된 분기정보가 나의 스택에서도 있었다면 삭제
                                with self.nnlock:
                                    if terminate_addr in self.nn_table:
                                        terminated_neighbor_table = self.nn_table.pop(terminate_addr)
                                        for a, [p, z, s, dp] in terminated_neighbor_table.items():
                                            self.n.neighbor_table[a] = [p, z, s, dp]
                                self.n.neighbor_update(self.z.zone)
                                with self.dplock:
                                    for dn, dd in tdp.items():
                                        self.datapoint_dict[dn]=dd
                                    self.datapoint_update_zdp()
                                if terminate_addr in self.n.neighbor_table:
                                    del(self.n.neighbor_table[terminate_addr])    ## nieghbor에서 삭제
                                with self.dplock:
                                    self.n.neighbor_table[self.this_addr] = [self.point, self.z.zone, self.s.get_Split_history(),self.datapoint_dict]
                                self.check = int(time.time())+300
                                msg = ('neighbor update', self.this_addr, self.n.get_neighbor_table())
                                for a in self.n.neighbor_table.keys():
                                    self.sender(msg,a)
                                if self.this_addr in self.n.neighbor_table:
                                    del(self.n.neighbor_table[self.this_addr])
                            else:
                                del(self.n.neighbor_table[terminate_addr])
            self.check=self.ctime + 300
                                

        
        elif msg[0] == 'neighbor list request' and not self.alone:
            header,a = msg
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            with self.nlock:
                msg = ('neighbor list', self.this_addr,self.n.neighbor_table)
                self.sender(msg,a)
        
        elif msg[0] == 'neighbor list' and not self.alone:
            nmsg,oa,n_list = msg
            with self.nlock:
                for na, [p, z, s, dp] in n_list.items():
                    self.n.neighbor_table[na] = [p, z, s, dp]
                self.n.neighbor_update(self.z.zone)

        elif msg[0] == 'info' and not self.alone:
            ##msg = ('heart beat',self.this_addr, self.z.zone, self.point, self.s.history, self.n.get_neighbor_table())
            header,neigh_addr,neigh_zone,neigh_point,neigh_history,neigh_neigh_table,neigh_datapoint = msg
            remove_nn =[]
            with self.tlock:
                self.timer[neigh_addr] = self.ctime
            with self.nlock:
                self.n.neighbor_table[neigh_addr] = [neigh_point,neigh_zone,neigh_history,neigh_datapoint]
                self.n.neighbor_update(self.z.zone)
                with self.nnlock:
                    self.nn_table[neigh_addr] = neigh_neigh_table
                    for na,nn in self.nn_table.items():     ##neighbor가 아닌 노드 nntable에서 삭제
                        if na not in self.n.neighbor_table.keys():
                            remove_nn.append(na)
                    for na in remove_nn:
                        del (self.nn_table[na])

        elif msg[0] == 'info request' and not self.alone:
            header, req_addr = msg
            with self.zlock:
                with self.slock:
                    with self.nlock:
                        with self.dplock:
                            msg = ('info', self.this_addr,self.z.zone, self.point, self.s.history, self.n.get_neighbor_table(),self.datapoint_dict)
                            self.sender(msg,req_addr)
            
        elif msg[0] == 'heart beat' and not self.alone:
            header, comparison_addr, comparison_data = msg
            self.ctime = int(time.time())
            with self.tlock:
                self.timer[msg[1]] = self.ctime
            with self.nlock:
                if comparison_addr in self.n.neighbor_table.keys():
                    with self.nnlock:
                        if comparison_addr not in self.nn_table.keys():
                            msg = ('info request', self.this_addr)
                            self.sender(msg,comparison_addr)
                        else:
                            check_data=(self.n.neighbor_table[comparison_addr][0],self.n.neighbor_table[comparison_addr][1],self.n.neighbor_table[comparison_addr][2],self.nn_table[comparison_addr].items(),self.n.neighbor_table[comparison_addr][3])
                            check_data = str(check_data)
                            hash_data = hashlib.md5(check_data.encode()).hexdigest()
                            if comparison_data != hash_data:
                                ##print('!wrong\n',self.this_addr, hash_data, comparison_addr, comparison_data)
                                msg = ('info request', self.this_addr)
                                self.sender(msg,comparison_addr)
                else:
                    msg = ('info request', self.this_addr)
                    self.sender(msg,comparison_addr)
        elif msg[0] == 'data add':
            header,og_addr,data_point,past_queue=msg
            with self.zlock:
                with self.nlock:
                    addr,past_queue = self.routing_zn(data_point,past_queue)
            if addr == self.this_addr:
                with self.dplock:
                    self.datapoint_dict[data_point]= og_addr
                print(data_point,'is in',self.this_addr)
                with self.zlock:
                    with self.nlock:
                        msg = ('neighbor update', self.this_addr, self.n.get_neighbor_table())
                        for a in self.n.neighbor_table.keys():
                            self.sender(msg,a)
            else:
                msg = ('data add', og_addr,data_point,past_queue)
                self.sender(msg,addr)
                
        elif msg[0] == 'data remove':
            header,og_addr,data_name,data_point,past_queue=msg
            with self.zlock:
                with self.nlock:
                    addr,past_queue = self.routing_zn(data_point,past_queue)
            if addr == self.this_addr:
                with self.dplock:
                    del(self.datapoint_dict[data_point])
                with self.zlock:
                    with self.nlock:
                        msg = ('neighbor update', self.this_addr, self.n.get_neighbor_table())
                        for a in self.n.neighbor_table.keys():
                            self.sender(msg,a)
            else:
                msg = ('data remove', og_addr,data_point,past_queue)
                self.sender(msg,addr)
             
        
        elif msg[0] == 'data search':
            header,og_addr,data_name,data_point,past_queue=msg
            with self.zlock:
                with self.nlock:
                    addr,past_queue = self.routing_zn(data_point,past_queue)
            if addr == self.this_addr:
                if data_point in self.datapoint_dict.keys():
                    print(self.this_addr,"own this data's address")
                    with self.dlock:
                        with self.dplock:
                            if self.datapoint_dict[data_point] == self.this_addr:
                                if data_point in self.data_dict:
                                    msg = ('data send',self.this_addr,data_name,self.data_dict[data_point])
                                    self.sender(msg,og_addr)
                                else:
                                    data_directoy = './share'
                                    data_path = os.path.join(data_directoy,data_name)
                                    if not os.path.exists(data_path):
                                        msg = ('data error', self.this_addr,data_name,'not exist')
                                        self.sender(msg,og_addr)
                                    else:        
                                        with open(data_path,'rb') as file:
                                            file_data = file.read()
                                            msg = ('file send',self.this_addr,data_name,file_data)
                                            self.sender(msg,og_addr)
                                        
                            else:
                                msg = ('data request', self.this_addr, og_addr, data_name)
                                self.sender(msg,self.datapoint_dict[data_point])
                else:
                    print("wrong dataname")
                    msg = ('data error', self.this_addr,'wrong data name')
                    self.sender(msg,og_addr)
            else:
                msg = ('data search',og_addr, data_name,data_point,past_queue)
                self.sender(msg,addr)
        
        elif msg[0] == 'data request':
            header,addr,og_addr,data_name = msg
            data_point = self.hash_func(data_name)
            with self.dlock:
                if data_point in self.data_dict:
                    msg = ('data send',self.this_addr,data_name,self.data_dict[data_point])
                    self.sender(msg,og_addr)
                else:
                    file_directoy = './share'
                    file_path = os.path.join(file_directoy,data_name)
                    if not os.path.exists(file_path):
                        msg = ('data error', self.this_addr,data_name,'not exist')
                        self.sender(msg,og_addr)
                    else:        
                        with open(file_path,'rb') as file:
                            file_data = file.read()
                            msg = ('file send',self.this_addr,data_name,file_data)
                            self.sender(msg,og_addr)
                
        elif msg[0] == 'data error':
            header,addr,data_name,err_msg = msg
            print('data_name ',err_msg,' from ',addr)
        
        elif msg[0] == 'file send':
            header,addr,data_name,file_data = msg
            save_path = os.path.join('./share',data_name)
            print(save_path)
            with open(save_path, 'wb') as file:
                file.write(file_data)
        
        elif msg[0] == 'data send':
            header,addr,data_name,file_data = msg
            data_point = self.hash_func(data_name)
            self.data_dict[data_point]=file_data
                
        elif msg[0] == 'check':
            while True:
                if self.ctime <= self.check:
                    continue
                n_zone = []
                with self.nlock:
                    for a,[p,z,s,d] in self.n.neighbor_table.items():
                        n_zone.append(z)
                with self.zlock:
                    if self.z.mini_eureka(n_zone,self.max_zone):
                        break
                    else:
                        self.check = self.ctime+20
                        msg = ('neighbor list request', self.this_addr)
                        with self.nlock:
                            for a in self.n.neighbor_table.keys():
                                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                                try:
                                    sock.connect(a)
                                    sock.sendall(pickle.dumps(msg))
                                    sock.close()
                                except Exception as e:
                                    print(f"An error occured: {e}",msg[0],'from',self.this_addr,'to',a)
        
        else:
            a=0

        if conn:
            conn.close
            
    def distance(self, a, b):
        self.a = a
        self.b = b
        self.d = 0
        for _ in range(len(a)):
            self.d += abs(self.a[_] - self.b[_])
        return self.d

    def hash_to_zone(self, identifier, dimensions, max_zone, seed):
        self.zone = []
        self.seed = seed
        # String creation by combining identifier and seed
        for i in range(dimensions):
            input_string = str(identifier[i]) + str(seed)

            # Convert to bytes using hash function
            hashed_bytes = hashlib.sha256(input_string.encode()).digest()

            # convert bytes to integer
            hashed_int = int.from_bytes(hashed_bytes, byteorder='big')
            zone = hashed_int % max_zone
            self.zone.append(zone)
            self.seed += 12

        return self.zone
