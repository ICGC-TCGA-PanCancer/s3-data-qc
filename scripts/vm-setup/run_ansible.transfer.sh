#!/bin/bash

for ip in `cat ip_address.txt`; 
    do echo $ip; 
    sed "s/[0-9]\{1,3\}.[0-9]\{1,3\}.[0-9]\{1,3\}.[0-9]\{1,3\}/$ip/g" inventory1.txt > inventory.txt; 
    cat inventory.txt; 
    # ansible-playbook install_collab_transfer.yml -i inventory.txt; 
    ansible-playbook install_aws_transfer.yml -i inventory.txt;
done

