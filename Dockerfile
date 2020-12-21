FROM golang:1.13-stretch AS builder
RUN mkdir /knivete
ADD . /knivete
WORKDIR /knivete

# Go env
RUN go env -w GOPROXY=https://proxy.golang.org
RUN go env -w CGO_ENABLED="0"
RUN go env -w GO111MODULE='on'

# Fetching dependencies
RUN go mod download

# Building
RUN go build -o bin/knivete -ldflags="-s -w" .


FROM alpine:latest
RUN mkdir -p /go/bin
WORKDIR /go/bin
COPY --from=builder /knivete/bin/knivete .