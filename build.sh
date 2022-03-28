#!/bin/bash
docker build -t wesport/odds-server .
docker tag wesport/odds-server:latest 989041579659.dkr.ecr.ap-northeast-1.amazonaws.com/wesport/odds-server:latest