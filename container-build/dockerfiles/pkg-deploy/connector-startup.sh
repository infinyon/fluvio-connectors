#!/bin/sh

set -eu

connector_name=${1:-"infinyon/http-source/0.1.0"}
cfgfile=${2:-"config.yaml"}

hub_host=${HUBURL:-http://192.168.106.2:8080}
# arch=${2:-'aarch64-linux-unknown-musl'}
auth_token=${AUTH_TOKEN:-""}

echo Connector Name ${connector_name}

# for now download public using "load" hack (w/ no auth for public packages)
pkgurl=${hub_host}/hub/v0/connector/load/${connector_name}

echo Loading ${pkgurl}
http_status=$(curl -o connector.ipkg -s -w "%{response_code}" \
  ${pkgurl} \
  -X GET \
  -H "Authorization: ${auth_token}"
  )
ls -l connector.ipkg

echo HTTP Response Status ${http_status}
if [ $http_status != "200" ]; then
	exit 1
fi

echo Starting cdk deploy with cfgfile=${cfgfile}

dev-cdk deploy start --ipkg ./connector.ipkg --config ${cfgfile}
