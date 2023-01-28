#!/bin/bash

aws ecr get-login-password --region ap-northeast-1 | sudo docker login --username AWS --password-stdin 989041579659.dkr.ecr.ap-northeast-1.amazonaws.com

echo "Building wesport odds server ...."
docker build -t wesport/odds-server .
docker tag wesport/odds-server:latest 989041579659.dkr.ecr.ap-northeast-1.amazonaws.com/wesport/odds-server:latest
docker push 989041579659.dkr.ecr.ap-northeast-1.amazonaws.com/wesport/odds-server:latest