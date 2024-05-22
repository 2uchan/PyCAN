""" Created on 2023
@author: Sookwang Lee (Korea Aerospace Univ)
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
import itertools

class Zone:
    def __init__(self, *zone):
        # Zone : [[dim1_lower, dim1_upper], [dim2_lower, dim2_upper], ....]
        self.zone = [list(zone[i:i+2]) for i in range(0, len(zone), 2)]
        # Zone centers : [dim1_center, dim2_center, ...]fl
        self.centers = [sum(c)/2 for c in self.zone]
        self.lower = [c[0] for c in self.zone]
        self.upper = [c[1] for c in self.zone]
        self.dim  = len(self.zone)

    def show(self):
        print("Node Zone:")
        for i in range(self.dim):
            print("Dimension", i,":")
            print("Lower:", self.lower[i], "Upper:", self.upper[i])
            print("Center:", self.centers[i])

    def contain(self, *args):
        # Check if the hash zone are included in the peer area.
        point = list(*args)
        return all(l <= x < u for l, u, x in zip(self.lower, self.upper, point))

    def isNeighbor(self, c):
        # Check if two peer zone are neighbors.
        for i in range(self.dim):
            if self.upper[i] == c.lower[i] or self.lower[i] == c.upper[i]:
                self_half_length = self.upper[i] - self.centers[i] 
                input_half_length = c.upper[i] - c.centers[i]
                center_length = abs(self.centers[i] - c.centers[i])
                if self_half_length + input_half_length == center_length:
                    if all(((self.lower[j] < c.upper[j] <= self.upper[j]) or (self.lower[j] <= c.lower[j] < self.upper[j]) or (self.lower[j] > c.lower[j] and self.upper[j] < c.upper[j])) for j in range(self.dim) if j != i):
                        return True
        return False

    def shared_axis(self,c):
        for shared_axis in range(self.dim):    ## which axis is orthogonal to the node
                   if self.upper[shared_axis] == c.lower[shared_axis] or self.lower[shared_axis] == c.upper[shared_axis]:
                        return shared_axis
        
    def is_mergeable(self, c):
        s_axis = self.shared_axis(c)
        for i in range(self.dim):
            if i == s_axis:
                continue
            if self.upper[i] >= c.upper[i] and self.lower <= c.lower[i]:
                continue
            else:
                return False
        else:
            return True

    def area(self):
        return np.prod([u-l for l, u in zip(self.lower, self.upper)])

    def isSameSize(self, c):
        # Check if both peers have the same realm size
        return all((u-l) == (c.upper[i]-c.lower[i]) for i, (l, u) in enumerate(zip(self.lower, self.upper)))

    def merge(self, c):
        # Merge each Neighbor to delete zone.
        if self.isNeighbor(c) == True:
            merge = [[min(l1, l2), max(u1, u2)] for i, ((l1, u1), (l2, u2)) in enumerate(zip(self.zone, c.zone))]
            return Zone(*(lu for axis in merge for lu in axis))

    def Split_Axis(self, origin_point, join_point, dimension):
        # Split peer zone to join zone setting.
        self.oh = origin_point
        self.jh = join_point
        self.dimension = dimension
        cut = self.centers
        fail = True
        self.point_max_distance = 0
        max_axis = 0
        
        if self.oh[:-1] == self.jh[:-1]:
            max_axis = self.dimension - 1
            print('same hash map')
        else :
            for axis in range(self.dimension):
                distance = abs(self.oh[axis] - self.jh[axis])
                if distance > self.point_max_distance:
                    self.point_max_distance = distance
                    max_axis = axis
             
        if (cut[max_axis]== self.jh[max_axis] or cut[max_axis] == self.oh[max_axis]):
            cut[max_axis] = cut[max_axis]+1

        while fail:
            # Execute loop to calculate axis zone between two point_max zone.
            if abs(self.oh[max_axis] - self.jh[max_axis]) == abs(self.oh[max_axis] - cut[max_axis]) + abs(self.jh[max_axis] - cut[max_axis]): #and self.oh[max_axis]!=cut[max_axis] and self.jh[max_axis]!=cut[max_axis] :
                # if cut axis exist between original hash zone with join hash zone, split zone to zone1, zone2
                zone1 = [list(t) for t in self.zone]
                zone2 = [list(t) for t in self.zone]
                int_cut = int(cut[max_axis])

                zone1[max_axis][1] = int_cut
                zone2[max_axis][0] = int_cut
                fail = False
                # store the dimension of the axis to be swapped
                change_axis = max_axis

            if fail:
                if self.upper[max_axis]-self.lower[max_axis] ==1:
                    cut[max_axis]=self.lower[max_axis]
                else:
                    while True:
                        cut[max_axis] = random.randint(int(self.lower[max_axis]), int(self.upper[max_axis])-1)
                        cut[max_axis] = int(cut[max_axis])
                        if self.oh[max_axis] != cut[max_axis] and self.jh[max_axis] != cut[max_axis]:
                            break
                    
                       


        # Separate the top and bottom of the divided zone value and return
        if self.oh[change_axis] - self.jh[change_axis] > 0:
            return zone2, zone1, max_axis, int_cut
        else:
            return zone1, zone2, max_axis, int_cut
    def getCoords(self):
        return self.zone

    def getLower(self):
        return self.lower

    def getUpper(self):
        return self.upper
    
    def orthogonal(self,point,max):
        check = 0
        min_dist=max
        for i in range(self.dim):
            if self.lower[i]<= point[i]<self.upper[i]:
                check+=1
            else:
                dist = min(abs(self.lower[i]-point[i]),abs(self.upper[i]-point[i]))
                if dist<min_dist:
                    min_dist=dist

        if check == self.dim:
            return True,0
        elif check == self.dim-1:
            return True,dist
        else:
            return False,0
    
    def vertex_dist(self,point,max):
        vertex = list(itertools.product(*self.zone))
        min_dist = max
        for i in vertex:
            dist = math.sqrt(sum([(a - b) ** 2 for a, b in zip(i, point)]))
            min_dist = min(min_dist, dist)
        return min_dist
    
    def mini_eureka(self,c_list,maxv):
        axis_space =[1]*self.dim
        touch_space = [0]*self.dim
        width = [upper-lower for upper, lower in zip(self.upper,self.lower)]
        for i in range(self.dim):
            if self.lower[i] == 0 and self.upper[i] == maxv:
                axis_space[i]=0
            if self.lower[i] == 0 or self.upper[i] == maxv:
                for k in range(self.dim):
                    if k != i:
                        axis_space[i]*=width[k]
            else:
                for k in range(self.dim):
                    if k != i:
                        axis_space[i]*=width[k]
                axis_space[i]*=2
        for c in c_list:
            clower = [cc[0] for cc in c]
            cupper = [cc[1] for cc in c]
            touch_low = [max(x,y) for x,y in zip(clower,self.lower)]
            touch_up = [min(x,y) for x,y in zip(cupper,self.upper)]
            cwidth = [upper-lower for upper, lower in zip(touch_up,touch_low)]
            for i in range(self.dim):
                if(self.lower[i]==c[i][1] or self.upper[i]== c[i][0]):
                    space =1
                    for k in range(self.dim):
                        if k !=i:
                            space *= cwidth[k]
                    break
                elif i == self.dim-1:
                    space = 0 
            touch_space[i] += space
        
        ax_space =0
        t_space =0
        for ax in range(self.dim):
            ax_space += axis_space[ax]
            t_space += touch_space[ax]
            
        while ax_space >= 10**10:
            ax_space //= 10
        while t_space >= 10**10:
            t_space //= 10
        return ax_space == t_space
    
    def eureka_check(self,n_list):
        past_n = []
        for ni in n_list.keys():
            past_n.append(ni)
            ni_zone = n_list[ni][1]
            for nj in n_list.keys():
                if nj in past_n:
                    continue
                nj_zone = n_list[nj][1]
                axis_check=0
                for axis in range(self.dim):
                    check_low = max(ni_zone[axis][0],nj_zone[axis][0])
                    check_up = min(ni_zone[axis][1],nj_zone[axis][1])
                    if(check_low<check_up):
                        axis_check+=1
                if(axis_check>=self.dim):
                    ni_size =0
                    nj_size=0
                    for l in range(self.dim):
                        print(ni[l][1])
                        ni_size *= (ni[l][1]-ni[l][0]) 
                        nj_size *= (nj[l][1]-nj[l][0])
                    if ni_size > nj_size:
                        return nj
                    else:
                        return ni
        return 0
                    
            
            
        


