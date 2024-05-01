#!/bin/bash


start_time=$(date '+%s')

M=$1
total_node_nums=$2
sever_index=$3
number_of_server=$4

parameter_array=("$@")
server_array=()

node_nums=$((total_node_nums / number_of_server))
ports=13000
node=$((sever_index * node_nums))

for ((i=4; i<${#parameter_array[@]}; i++)); do
    server_array+=("${parameter_array[i]}")
done

echo Parameter initialize done! Mode $M, node scale $total_node_nums, number of server $number_of_server
echo "${server_array}"
sleep 1

#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Mode 1 is initialize Bootstrap and join nodes
if [ $M -eq 1 ];
then 
echo Mode 1 start

pkill -9 python3
source ~/anaconda3/bin/activate base

# Bootstrap ~ Node(node_nums -1) join
while [ $node -lt $node_nums ]; do
    if [ $ports = '13000' ]
    then 
    (SERVER_ARRAY=$(IFS=,; echo "${server_array[*]}") python3 Main.py --bootstrap=True --port=$ports --node_num=$node) &
    echo Bootstrap initialized!
    ((ports++))
    ((node++))
    sleep 1

    else
    echo Node $node join!
    (SERVER_ARRAY=$(IFS=,; echo "${server_array[*]}") python3 Main.py --port=$ports --node_num=$node) &
    while true; do
        if tail -n 1 log.txt | grep -q "Queue reset!($node)"; then
            ((ports++))
            ((node++))
            break
        fi
        sleep 1
    done
    fi
done
# server_array index++
((sever_index++))
echo "${server_array[$sever_index]} , $sever_index"
# send to next server(Mode 2) to join Nodes
ssh -p 6304 deepl@${server_array[$sever_index]} "source ~/anaconda3/bin/activate base; cd yuchan/CAN; ./joining.sh 2  $total_node_nums $sever_index $number_of_server ${server_array[@]}"

#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Mode 2 is Node join from other servers and last server send Mode 3 to original server 
elif [ $M -eq 2 ];
then
echo Mode 2 start

if [ $sever_index -lt $number_of_server ];
then
pkill -9 python3
ip_node_nums=`expr "$sever_index" \* "$node_nums"` 
ports=`expr "$ports" + "$ip_node_nums"`
node=$ip_node_nums

while [ $node -lt `expr "$node_nums" + "$ip_node_nums"` ]; do
    echo Node $node join!
    SERVER_ARRAY=$(IFS=,; echo "${server_array[*]}") python3 Main.py --port=$ports --node_num=$node &
    while true; do
        if tail -n 1 log.txt | grep -q "Queue reset!($node)"; then
            ((ports++))
            ((node++))
            break
        fi
        sleep 1
    done
done

# server_array index ++ 
((sever_index++))

if [ $sever_index -ne $number_of_server ];
then
# send to next server(Mode 2) to join Nodes
ssh -p 6304 deepl@${server_array[$sever_index]} "source ~/anaconda3/bin/activate base; cd yuchan/CAN; ./joining.sh 2  $total_node_nums $sever_index $number_of_server ${server_array[@]}"

fi

else
echo Last server join done! Please check your parameters

fi

echo done!
fi

end_time=$(date '+%s')

diff=$((end_time - start_time))
hour=$((diff / 3600 % 24))
minute=$((diff / 60 % 60))
second=$((diff % 60))

echo "$hour hour $minute minute $second second"