#! /usr/bin/bash

echo "*** [RUN]: plasma_store -m 100000000000 -s /tmp/plasma ***"
echo

# 100,000,000,000 == 100 GB
/home/fanguy/.local/bin/plasma_store -m 100000000000 -s /tmp/plasma

echo
