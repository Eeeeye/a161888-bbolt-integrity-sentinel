FROM golang:1.25.8-bookworm

WORKDIR /app

ENV GOTOOLCHAIN=local

COPY bbolt/ /app/
RUN mkdir -p /logs/verifier \
    && go build . ./internal/surgeon

ENV GOPROXY=off
ENV GOSUMDB=off

USER root
