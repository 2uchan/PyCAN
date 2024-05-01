# pyCAN
Multi dimensinonal Content Addressable Network implemented by Python

## Introduce
We have implemented a scalable content addressable network (CAN) capable of organizing nodes in a multidimensional space and allowing for the addition and deletion of nodes.

## Develop period
* 23.06.12 ~ present

## Participating workforce
 - Master's research student  : Sookwang Lee - CAN code configuration
 - undergraduate research student : Yuchan Lee - CAN node verification code (eureka), CAN code configuration

## Development environment
- `Python 3.7.4`
- `Shell script`
- **OS** : Ubuntu

## Config file
Please modify the config file according to your environment.

- setting.json
- `host_addr` : Bootstrap's ip address
- `host_port` : Bootstrap's port number
- `dimension` : The dimension of CAN space
- `max_zone`  : The size of space

## How to use
  ### Create a node
  - run Main.py
    **Parameter value**
      - `port` : This node's port
      - `bootstrap` : Is this node Boostrap? (boolean)
      - `node_num` : This node's number
  
    ### Example 
      **Bootstrap**
      ```
      python3 Main.py --port 13000 --bootstrap True --node_num 0
      ```
  
      **Node**
      ```
      python3 Main.py --port 13001 --bootstrap False --node_num 1
      ```
  
  ### Generate multi nodes
  - run joining.sh
  In joining.sh, allocate port 13000 to the bootstrap, then incrementally assign port numbers to new nodes, increasing by 1 for each node. If you want to change the bootstrap port, modify the "ports" variable in joining.sh, and update the "host_port" in seeing.json accordingly.

    **Parameter value**
    - `mode` : 1(Bootstrap+Nodes), 2(Nodes)
    - `total node num` : The number of nodes to be created
    - `server index` : To initialize the index for partitioning across multiple servers, please enter 0 when starting the shell script.
    - `number of server` : The number of servers on which nodes will be created
    - `server array` : Ip addresses of the servers
  
    **Example**
    ```
    ./joining.sh 1 500 0 5 '220.11.111.100' '220.11.111.101' '220.11.111.102' '220.11.111.103' '220.11.111.103'
    ```

## API
```
node = NodeBase(args)
threading.Thread(target=node.firstjob, daemon=True).start() 

node.data_add(data_name, data_content)
node.file_add(file_name)

node.data_remove(data_name)

node.data_search(data_name)
```

## Verification
Verification code can only be used in systems configured using joining.sh.
Run scan.py to generate logs and execute eureka.py to verify that the system has been set up stably.
- run scan.py on each server
  
 **Parameter value**
   - `removed_port` : The port number of the deleted node for handling exceptions related to deleted nodes.
   - `range_start` : The lowest port number among the configured nodes on the server.
   - `range end` : The highest port number among the configured nodes on the server.
   - `address` : Ip address of the server

- After consolidating the logs into a server, run eureka.py
  
   **Parameter value**
     - `removed_num` : The node number of the deleted node for handling exceptions related to deleted nodes. Confirm that it's the node number, not the port number. 
     - `dimension` : The dimension of CAN space
     - `max_zone`  : The size of space
     - `node_nums` : The total number of nodes
     - `route` :  The path where the logs are stored


  
  
