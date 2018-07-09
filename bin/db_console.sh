#!/bin/bash

wait_tcp_port() {
  local host="$1" port="$2"

  # see http://tldp.org/LDP/abs/html/devref1.html for description of this syntax.
  while ! exec 6<>/dev/tcp/$host/$port; do
    echo "$(date) - still trying to connect to $host:$port"
    sleep 1
  done
  exec 6>&-
}

teardown() {
  echo "Shutting down cloud-sql-proxy..."
  pkill cloud-sql-proxy
}

proxy_up() {
  local DBNAME=$1
  if [[ -z $DBNAME ]]; then
    DBNAME="dev-bizops"
  fi

  cloud-sql-proxy -instances=gitlab-analysis:us-west1:$1=tcp:6543 &
}

dev() {
  PGPASSWORD="${PG_PASSWORD}" psql "host=${PG_ADDRESS} port=${PG_PORT} dbname=${PG_DATABASE} user=${PG_USERNAME}"
}

staging() {
  local DBUSER=${1:-$USER}

  proxy_up "dev-bizops"
  wait_tcp_port localhost 6543
  psql "host=localhost port=6543 dbname=dw_production" ${DBUSER}
}

production() {
  local DBUSER=${1:-$USER}

  proxy_up "bizops"
  wait_tcp_port localhost 6543
  psql "host=localhost port=6543 dbname=dw_production" ${DBUSER}
}

review() {
  local INSTANCE=$1
  local DBUSER=${2:-$USER}

  if [[ -z "$INSTANCE" ]]; then
    echo "No instance specified, aborting."
    return 1
  fi

  proxy_up "$INSTANCE"
  wait_tcp_port localhost 6543
  psql "host=localhost port=6543 dbname=dw_production" ${DBUSER}
}

# kill this process tree
trap "teardown" INT TERM EXIT

if [[ -z $1 || "$1" = "dev" ]]; then
  echo "Connecting to ${PG_USERNAME}@${PG_ADDRESS}/${PG_DATABASE}..."
  dev
elif [[ "$1" = "staging" ]]; then
  echo "Connecting to staging..."
  staging ${2:-$PG_STAGING_USERNAME}
elif [[ "$1" = "production" ]]; then
  echo "Connecting to production..."
  production $2 # <user>
elif [[ "$1" = "review" ]]; then
  echo "Connecting to review..."
  review $2 $3 # <instance> <user>
fi

