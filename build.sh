#!/usr/bin/env bash

build_date=$(date -u +%Y%m%d.%H%M%S)
git_rev=$(git rev-parse --short HEAD)

GOOS=linux GOARCH=amd64 go build -ldflags "-X main.VersionBuild=${git_rev} -X main.BuildDate=${build_date}" -o build/scriba.linux
GOOS=darwin GOARCH=amd64 go build -ldflags "-X main.VersionBuild=${git_rev} -X main.BuildDate=${build_date}" -o build/scriba.macos

