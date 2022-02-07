#!/bin/bash
set -e
for i in {1..10}; do
    echo Checking kd3 status for the $i time.  $(k3d node list -o json | jq '.[].State')
    if [[ $(k3d node list -o json | jq '.[].State.Running' | uniq -c) = '      2 true' ]]; then
        exit 0
    fi
    sleep 5
done
echo "Cluster failed to start"
exit 1
