#!/usr/bin/env bash
set -o errexit
set -o nounset
set -o xtrace

lsblk | awk '{ if ($6 == "disk") print "/dev/"$1 }' | xargs -I {} bash -c 'if [ "`file -s -b {}`" == "data" ] ; then echo {} ; fi'
