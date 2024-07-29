""" Created on 2023
@author: Yuchan Lee (Korea Aerospace Univ)
"""
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
import hashlib
import copy
import sys
import os
import re
from matplotlib import pyplot as plt

class Eureka:
    def __init__(self):
        self.dim = args.dimension
        self.axis_max = args.max_coordinate
        self.removed_num = args.removed_num if isinstance(args.removed_num, (list, tuple)) else [args.removed_num]
        self.sucess = False
        self.nodes = args.node_nums
        self.axis_max_len = len(str(self.axis_max))
        self.table = {}
        self.error_count = 0


        for node in range(self.nodes):   ##append node's information to the table
            if node in self.removed_num:
                continue
            print(node)
            node_file="Node%d.txt"%(node)
            file_name=args.route + "/"+node_file
            f = open(file_name,"r")
            coord=[]
            hashmap=[]
            neigh={}
            stack=[]
            nn={}
            di={}
            for i in range(6):
                line = f.readline()
                if(i==0):
                    addr = eval(line)
                elif(i==1):	##append own coordination
                    coord = eval(line)
                elif(i==2):	##append own hashmap
                    hashmap = eval(line)
                elif(i==3):
                    stack = eval(line)
                elif(i==4):
                    dp = eval(line)
                elif(i==5):
                    neigh = eval(line)
                else:
                    nn= eval(line)
            di['hashmap'] = hashmap
            if(len(hashmap)!=self.dim):		##notice if the hashmap information is different from dimension information
                print("Node%d's hashmap information is different from dimension  Node%d's hashmap dimension: %d  provided dimension: %d"%(node,node,len(hashmap),self.dim))
                print(hashmap)
            di['coord'] = coord  
            if(len(coord)!=self.dim):
                print("Node%d's coordinate information is different from dimension  Node%d's coordinate dimension: %d  provided dimension: %d"%(node,node,len(coord),self.dim))
                print(coord)
            di['stack'] = stack
            di['dp'] = dp
            di['neighbor'] = neigh
            di['nn'] = nn
            self.table[addr] = di
    
        for my_addr, my_di in self.table.items():
            for check_addr,[ch,cc,cs,cdp] in my_di['neighbor'].items():
                if(self.table[check_addr]['hashmap']!=ch):
                    print('\n','!'*10,my_addr,'has wrong hashmap of', check_addr)
                    print('\noriginal hashmap',self.table[check_addr]['hashmap'],'\n wrong hashmap',ch)
                if(self.table[check_addr]['coord']!=cc):
                    print('\n','!'*10,my_addr,'has wrong coordinate of', check_addr)
                    print('\noriginal coordinate',self.table[check_addr]['coord'],'\n wrong coordinate',cc)
                if(self.table[check_addr]['stack']!=cs):
                    print('\n','!'*10,my_addr,'has wrong stack of', check_addr)
                    print('\noriginal stack',self.table[check_addr]['stack'],'\n wrong stack',cs)
                if(my_addr not in self.table[check_addr]['neighbor'].keys()):
                    print('\n','!'*10,my_addr,'is not neighbor of', check_addr)
                    print('\n',check_addr,"'s neighbor" ,self.table[check_addr]['neighbor'].keys(),'\n there is no',my_addr)
                if(self.table[check_addr]['dp']!=cdp):
                    print('\n','!'*10,my_addr,'has wrong datapoint of', check_addr)
                    print('\noriginal datapoint',self.table[check_addr]['dp'],'\n wrong datapoint',cdp)
            
            for check_addr,check_table in my_di['nn'].items():
                if check_addr not in self.table[my_addr]['neighbor'].keys():
                    print('\n','!'*10,my_addr,'has wrong nntable')
                    print(check_addr,'is not neighbor any more, but it is in the nn table of', my_addr)
                    continue
                if(self.table[check_addr]['neighbor']!= check_table):
                    print('\n','!'*10,my_addr,'has wrong neighbor table of', check_addr)
                    print('\noriginal neighbor table',self.table[check_addr]['neighbor'],'\n wrong neihgbor table',check_table)
    
        ## coord, hash가 neightable과 원래 정보가 같은지 확인

        self.position()
        self.entire_space()
        self.eureka()
        #self.coord_stats()
        self.neigh_stats()

    def position(self):	##check hashmap is located inside the coordinates

        for i,k in self.table.items():  
            hashmap = self.table[i]['hashmap']
            coord = self.table[i]['coord']

            for j in range(self.dim):
                if(coord[j][0]<=hashmap[j]<coord[j][1]):
                    pass
                else:
                    self.error_count +=1
                    print("\n",i,"'s hashmap shared_axis%d is out of boundary"%(j))
                    print(coord)
                    print(hashmap)
                    
            for dp in self.table[i]['dp'].keys():
                for j in range(self.dim):
                    if(coord[j][0]<=dp[j]<coord[j][1]):
                        pass
                    else:   
                        print("\n",i,"'s datapoint axis%d is out of boundary"%(j))
                        print(coord)
                        print(dp) 
        print("position check done")

    def entire_space(self): ##check that the sum of the nodes matches the size of the entire space
        space =0
        real_space = self.axis_max**self.dim
        for i,k in self.table.items():
            cal_space = 1
            node_coord = self.table[i]['coord']
            for j in range(self.dim):
                cal_space *= (node_coord[j][1]-node_coord[j][0])

            space += cal_space
        while space >= 10**10:
            space //= 10
        while real_space >= 10**10:
            real_space //= 10
        if(space!=real_space):
            print('\nreal_space=',real_space)
            print('space=',space)
            print("there is empty space")

        print('\nreal_space=',real_space)
        print('space=',space)
        print('entire space check done')

    def eureka(self):

        for i in self.table.keys():
            my_coord = self.table[i]['coord']
            my_neighbor = self.table[i]['neighbor']
            axis_space =[0]*self.dim
            touch_space = [0]*self.dim
            my_space = 1
            for k in range(self.dim):        ## total size of node
                my_space *= (my_coord[k][1]-my_coord[k][0])
            for k in range(self.dim):        ## size of outer shell of a node
                if(my_coord[k][0]==0 and my_coord[k][1]==self.axis_max):	## the lower and upper of the coordinate are both in contact with the boundary
                    axis_space[k] = 0
                elif(my_coord[k][0]==0 or my_coord[k][1]==self.axis_max):	## the lower or upper of the coordinate touches the boundary
                    axis_space[k] = my_space/(my_coord[k][1]-my_coord[k][0])
                elif(my_coord[k][0]==my_coord[k][1]):       ## 이건 지우기
                    axis_space[k]=0
                else:								## the lower and upper of the coordinate are not at the boundary
                    axis_space[k] = my_space*2/(my_coord[k][1]-my_coord[k][0])
                    
            for j in my_neighbor.keys():   
                neigh_coord = self.table[j]['coord']
                for shared_axis in range(self.dim):    ## which axis is orthogonal to the node
                    if(my_coord[shared_axis][0] == neigh_coord[shared_axis][1] or my_coord[shared_axis][1]==neigh_coord[shared_axis][0]):
                        break 
                    elif(shared_axis==(self.dim-1)):	## Node and neighbor node are not touch to each other
                        self.error_count +=1
                        print('\nneigh: ',neigh_coord)
                        print('node: ', my_coord)
                        print("Node",j,"is out of touch with Node",i)
                shared_coord = 1

                for axis in range(self.dim):
                    length = 0
                    if (axis==shared_axis): ## not calculating the orthogonal axis
                        continue
                    
                    touch_low = max(my_coord[axis][0],neigh_coord[axis][0])
                    touch_up = min(my_coord[axis][1],neigh_coord[axis][1])

                    if(touch_low > touch_up):
                        print('\nneigh: ',neigh_coord)
                        print('node: ', my_coord)
                        print('up: %d low:%d'%(touch_up,touch_low))
                        print('length: ', touch_up-touch_low)
                        print("check Node",i,"is touch with Node",j)
                        continue
                    length=touch_up-touch_low
                    shared_coord *= length
                touch_space[shared_axis]+=shared_coord

            for j in range(self.dim):
                while touch_space[j] >= 10**10:
                    touch_space[j] //= 10
                while axis_space[j]>= 10**10:
                    axis_space[j] //= 10
                if(touch_space[j]<axis_space[j]):
                    print("\nneighbor touch space: ",touch_space[j])
                    print("real boundary space: ", axis_space[j])
                    print("Node",i,"'s axis%d has smaller touch space than real boundary space\n"%(j))
                    for k in my_neighbor.keys():     
                        neigh_coord = self.table[k]['coord']
                        for shared_axis in range(self.dim):    ## which axis is orthogonal to the node
                            if(my_coord[shared_axis][0] == neigh_coord[shared_axis][1] or my_coord[shared_axis][1]==neigh_coord[shared_axis][0]):
                                break 
                        if shared_axis == j:
                            print('axis',j,k)

                elif(touch_space[j]>axis_space[j]):
                    print("!!!!!!!!!!!overlap",i,'axis',j)
                    print(touch_space[j])
                    print(axis_space[j])
                    neigh_list = list(my_neighbor.keys())
                    for ni in range(len(neigh_list)):
                        n1 = neigh_list[ni]
                        n1_coord=self.table[n1]['coord']
                        
                        for nj in range(ni+1,len(neigh_list)):
                            n2 = neigh_list[nj]
                            n2_coord=self.table[n2]['coord']
                            axis_check=0

                            for axis in range(self.dim):
                                check_low = max(n1_coord[axis][0],n2_coord[axis][0])
                                check_up = min(n1_coord[axis][1],n2_coord[axis][1])
                                if(check_low<check_up):
                                    axis_check+=1

                            if(axis_check>=self.dim):
                                print("\nNode",n1,"is overlapped with Node",n2)
                    for k in my_neighbor.keys():     
                        neigh_coord = self.table[k]['coord']
                        for shared_axis in range(self.dim):    ## which axis is orthogonal to the node
                            if(my_coord[shared_axis][0] == neigh_coord[shared_axis][1] or my_coord[shared_axis][1]==neigh_coord[shared_axis][0]):
                                break 
                        if shared_axis == j:
                            print('axis',j,k)
                            
        print("eureka done")

    def neigh_stats(self):	##distribution of how many neighbors each node has
        stats=[]
        boot = True
        for node in self.table.keys():
            num_neigh=len(self.table[node]['neighbor'])
            stats.append(num_neigh)
            if boot:
                print(num_neigh)
                boot = False
        avg = sum(stats)/len(stats)    
        print('avg:',avg)
        plt.hist(stats,bins=50)
        plt.title('the distribution of neighbors')
        plt.xlabel('the number of neighbors')
        plt.ylabel('the number of nodes')
        plt.savefig('neigh.png')
        plt.close()
        plt.bar(range(self.nodes),stats)
        plt.xlabel('Node num')
        plt.ylabel('the number of neighbors')
        #plt.title('Comunication volume of Bootstrap (build up phase)')
        plt.title('the number of neighbors per node')
        plt.savefig('neigh_size.png')


    def coord_stats(self):	## size distribution of nodes
        stats=[]
        for node in self.table.keys():
            coord = self.table[node]['coord']
            size=1
            for i in range(self.dim):
                length = coord[i][1]-coord[i][0]
                size *= length
            size = size/1000000000000
            stats.append(size)
        plt.hist(stats,bins=100)
        plt.xscale('log')
        plt.title('the distribution of size')
        plt.xlabel('size of node(X10^12)')
        plt.ylabel('the number of nodes')
        plt.savefig('size.png')
        plt.close()

if __name__ == '__main__':

    this_ip = socket.gethostbyname(socket.gethostname())
    parser = argparse.ArgumentParser(description="eureka")
    parser.add_argument("--removed_num","-rn", nargs='*', type = int, default=10000)
    parser.add_argument('-d','--dimension',type=int,default= 7)
    parser.add_argument('-m','--max_coordinate',type=float,default=65536)
    parser.add_argument('-n','--node_nums',type=int,default=200)
    parser.add_argument('-r','--route',default="/home/user/pyCAN/log")
    args=parser.parse_args()

    print('Eureka Start!')
    Eureka()
