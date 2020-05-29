#!/usr/bin/env bash

build_ver=$(date -u +%Y%m%d.%H%M%S)

GOOS=linux GOARCH=amd64 go build -ldflags "-X main.VersionBuild=${build_ver}" -o scriba.linux
GOOS=darwin GOARCH=amd64 go build -ldflags "-X main.VersionBuild=${build_ver}" -o scriba.macos
