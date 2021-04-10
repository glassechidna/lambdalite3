#!/bin/bash
set -euxo pipefail

export CGO_ENABLED=1
export GOOS=linux
export CC=$(pwd)/zcc

go build -ldflags="-s -w"

#docker build -t lambdalite3 .
#cid=$(docker create lambdalite3)
#docker cp ${cid}:/app/lambdalite3 .
#docker rm ${cid}

#stackit up --region ap-southeast-2 --profile ge --template infra.yml --stack-name lambdalite3-demo
