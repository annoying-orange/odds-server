FROM golang:alpine3.13 AS build
RUN apk --no-cache add gcc g++ make git ca-certificates

WORKDIR /go/src/github.com/wesport/odds-server
COPY . .
RUN go get -d -v
RUN GOOS=linux GOARCH=amd64 go build -a -v -tags musl -o /go/bin/oddssrv .

FROM alpine:3.13
WORKDIR /usr/bin
ENV PORT=8080
ENV REDIS=localhost:6379
COPY --from=build /go/bin .
EXPOSE $PORT
ENTRYPOINT oddssrv --redis $REDIS