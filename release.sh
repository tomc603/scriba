#!/usr/bin/env bash

set -e

declare VERSION_RE="([0-9]+)\.([0-9]+)\.([0-9]+)"
declare VERSION_DATA
declare -i VER_CUR_MAJOR
declare -i VER_CUR_MINOR
declare -i VER_CUR_POINT
declare -i VER_NEXT_MAJOR
declare -i VER_NEXT_MINOR
declare -i VER_NEXT_POINT

function usage() {
  echo -e "\n$(basename $0) - Create a versioned release for the current project in Major.Minor.Point format." 1>&2
  echo -e "\n  Usage:" 1>&2
  echo "  $(basename $0) [-a|--major] [-h|--help] [-i|--minor] [-p|--point]" 1>&2
  echo "    -a|--major    Increase the Major version value." 1>&2
  echo "    -h|--help     Display this help message." 1>&2
  echo "    -i|--minor    Increase the Minor version value." 1>&2
  echo "    -p|--point    Increase the Point version value. Default." 1>&2
}

if [[ ! -e VERSION ]]; then
  echo "ERROR: File VERSION does not exist." 1>&2
  exit 2
fi

VERSION_DATA=$(cat VERSION)
if [[ -z ${VERSION_DATA} ]]; then
  echo "WARNING: Unable to read version string from version file. Initializing to 0.0.0." 1>&2
  echo "0.0.0" > VERSION
  VERSION_DATA=$(cat VERSION)
fi

if [[ $VERSION_DATA =~ $VERSION_RE ]]; then
#  echo "${BASH_REMATCH[@]}"
  VER_CUR_MAJOR="${BASH_REMATCH[1]}"
  VER_CUR_MINOR="${BASH_REMATCH[2]}"
  VER_CUR_POINT="${BASH_REMATCH[3]}"
  VER_NEXT_MAJOR="${VER_CUR_MAJOR}"
  VER_NEXT_MINOR="${VER_CUR_MINOR}"
  VER_NEXT_POINT="${VER_CUR_POINT}"
fi

if [[ -z $1 ]]; then
  VER_NEXT_POINT+=1
else
  while [[ -n $1 ]]; do
    param=$1
    case ${param} in
      -a|--major)
        shift
        VER_NEXT_MAJOR+=1
        ;;
      -i|--minor)
        shift
        VER_NEXT_MINOR+=1
        ;;
      -p|--point)
        shift
        VER_NEXT_POINT+=1
        ;;
      -h|--help)
        shift
        usage
        exit 0
        ;;
      -*)
        shift
        echo "ERROR: Unknown option \"${param}\"" 1>&2
        usage
        exit 1
        ;;
    esac
  done
fi

echo "${VER_CUR_MAJOR}.${VER_CUR_MINOR}.${VER_CUR_POINT} -> ${VER_NEXT_MAJOR}.${VER_NEXT_MINOR}.${VER_NEXT_POINT}" 1>&2
echo "${VER_NEXT_MAJOR}.${VER_NEXT_MINOR}.${VER_NEXT_POINT}" > VERSION
