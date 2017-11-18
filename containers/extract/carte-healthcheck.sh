#!/bin/bash
set -e

# check kettle status
KETTLE_STATUS_RESPONSE=$(curl -S "http://$CARTE_USER:$CARTE_PASSWORD@$HOSTNAME:$CARTE_PORT/kettle/status/?xml=Y")

if [ $? -ne 0 ]; then
    echo $KETTLE_STATUS_RESPONSE;
    exit 1;
fi

sed -n -e 's/.*<statusdesc>\(.*\)<\/statusdesc>.*/\1/p' <<< $KETTLE_STATUS_RESPONSE
