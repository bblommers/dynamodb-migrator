#!/bin/bash
if [[ $(git diff --stat) != '' || -n $(git status -s) ]]; then
  exit 1
else
  exit 0
fi