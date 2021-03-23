#!/usr/bin/env bash

declare VERSION_RE="([0-9]+)\.([0-9]+)\.([0-9]+)"
declare VERSION_DATA
declare -i VER_CUR_MAJOR
declare -i VER_CUR_MINOR
declare -i VER_CUR_POINT

declare build_date=$(date -u +%Y%m%d.%H%M%S)
declare git_rev=$(git rev-parse --short HEAD)

if [[ ! -e VERSION ]]; then
  echo "ERROR: File VERSION does not exist." 1>&2
  exit 2
fi

VERSION_DATA=$(cat VERSION)
if [[ -z ${VERSION_DATA} ]]; then
  echo "ERROR: Unable to read version string from version file." 1>&2
fi

if [[ $VERSION_DATA =~ $VERSION_RE ]]; then
  VER_CUR_MAJOR="${BASH_REMATCH[1]}"
  VER_CUR_MINOR="${BASH_REMATCH[2]}"
  VER_CUR_POINT="${BASH_REMATCH[3]}"
fi

if [[ -z ${VER_CUR_MAJOR} || -z ${VER_CUR_MINOR} || -z ${VER_CUR_POINT} ]]; then
  echo "ERROR: Malformed version ${VER_CUR_MAJOR}.${VER_CUR_MINOR}.${VER_CUR_POINT}"
fi

echo "Building ${VER_CUR_MAJOR}.${VER_CUR_MINOR}.${VER_CUR_POINT}, ${git_rev}, ${build_date}" 1>&2
GOOS=linux GOARCH=amd64 go build -ldflags "-X main.VersionMajor=${VER_CUR_MAJOR} main.VersionMinor=${VER_CUR_MINOR} main.VersionPoint=${VER_CUR_POINT} main.VersionTag=${git_rev} -X main.BuildDate=${build_date}" -o build/scriba.linux
GOOS=darwin GOARCH=amd64 go build -ldflags "-X main.VersionMajor=${VER_CUR_MAJOR} main.VersionMinor=${VER_CUR_MINOR} main.VersionPoint=${VER_CUR_POINT} main.VersionTag=${git_rev} -X main.BuildDate=${build_date}" -o build/scriba.macos
