FROM golang:alpine3.15 AS build

RUN apk --no-cache add gcc g++ make git ca-certificates

WORKDIR /go/src/github.com/wesport/odds-server
COPY . .

RUN go get -d -v
RUN GOOS=linux GOARCH=amd64 go build -a -v -tags musl -o /go/bin/oddssrv .

FROM alpine:3.15

WORKDIR /usr/bin

ENV PORT=9090
ENV KAFKA_BOOTSTRAP_SERVERS localhost:9092
ENV REDIS_SENTINELS localhost:26379,localhost:26380,localhost:26381
ENV REDIS_PASSWORD P@ssw0rd!
ENV REDIS_DATABASE 1


COPY --from=build /go/bin .
EXPOSE $PORT

ENTRYPOINT oddssrv