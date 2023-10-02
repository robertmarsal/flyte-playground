#!/usr/bin/env bash

docker build -t localhost:30000/retrieve-books:latest -f images/retrieve-books.dockerfile .
docker push localhost:30000/retrieve-books:latest
