#!/bin/zsh

# After broker container start to install tree
# apt-get update && apt-get install tree

# To get a shell on the broker container
function docker-shell() {
  docker exec -it broker /bin/bash
}

function broker-logs() {
  docker logs broker
}

function sr-logs() {
  docker logs schema-registy
}

function get-subjects() {
  curl -s "http://localhost:8081/subjects" | jq
}

function get-version() {
  if [ -z "$1" ];
   then echo "You need to supply the subject-name"
   return
  fi
  curl -s "http://localhost:8081/subjects/{$1}/versions" | jq
}

function get-latest-schema() {
  if [ -z "$1" ];
   then echo "You need to supply the subject-name"
   return
  fi
  curl -s "http://localhost:8081/subjects/{$1}/versions/latest" | jq '.'
}

function consume() {
  if [ -z "$1" ];
   then echo "Must supply a topic name"
   return
  fi 

  echo "Starting to consume from $1"
  docker exec -it broker kafka-console-consumer --bootstrap-server localhost:9092\
   --from-beginning --property print.key=true --property key.separator="-" --topic "$1"
}

function list-topics() {
  echo "Kafka topics:"
  docker exec -it broker kafka-topics --list --bootstrap-server localhost:9092
}

