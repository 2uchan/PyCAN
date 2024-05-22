
class Split:
    def __init__(self, s_axis, s_cut):
        self.axis = s_axis
        self.cut = s_cut
        self.history = [[s_axis,s_cut]]

    def push(self,val):
        self.history.append(val)

    def pop(self):
        if not self.is_empty():
            return self.history.pop()
        else:
            print("split history is empty")
            return None

    def erase(self,info):
        if info in self.history:
            self.history.remove(info)

    def get_Split_history(self):
        return self.history
    
    def valid(self,z,t_z):
        while self.history:
            split_data=self.history.pop()
            split_axis = split_data[0]
            split_cut = split_data[1]
            for shared_axis in range(len(z)):    ## which axis is orthogonal to the node
                if(z[shared_axis][0] == t_z[shared_axis][1] or z[shared_axis][1]==t_z[shared_axis][0]):
                    break 
            if shared_axis != split_axis:
                break
            if t_z[split_axis][0] != split_cut and t_z[split_axis][1] != split_cut:
                continue
            
            if z[split_axis][0] == split_cut:       ## lower가 upper랑 같은경우
                if z[split_axis][0] == z[split_axis][1]:
                    z[split_axis][0] = t_z[split_axis][0]
                    z[split_axis][1] = t_z[split_axis][1]
                    return True,z,split_data
                else:
                    z[split_axis][0] = t_z[split_axis][0]
                    return True,z,split_data
            elif z[split_axis][1] == split_cut:
                z[split_axis][1] = t_z[split_axis][1]
                return True,z,split_data
            else:
                return False,z,split_data
        return False,z,split_data
                
        
            
                    



        
    
