#!/bin/bash

if [[ "$#" -lt 1 ]]; then
  echo "missing size"
  exit 1
fi

FILE_NAME="$1M.dat"
SIZE=$(( $1 * 1024 ))

dd if=/dev/urandom of="$FILE_NAME" bs=1024 count="$SIZE"
