FROM golang:1.25.8-bookworm

WORKDIR /app

ENV GOTOOLCHAIN=local

COPY bbolt/go.mod bbolt/go.sum ./
RUN go mod download

COPY bbolt/ /app/
RUN mkdir -p /logs/verifier \
    && go build ./...

ENV GOPROXY=off
ENV GOSUMDB=off

USER root
