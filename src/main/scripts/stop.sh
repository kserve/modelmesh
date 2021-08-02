#!/bin/bash

# Copyright 2021 IBM Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# takes an optional "wait" command line parameter in which case it will block
# until the main model-mesh container has exited

SCRIPT_DIR=$(dirname "$0")
ANCHOR_FILE="${SCRIPT_DIR}/model-mesh.anchor"

LL_PID=$(cat ${ANCHOR_FILE})

# remove the anchor, will trigger controlled shutdown
rm -f ${ANCHOR_FILE}

if [ \( -n "$LL_PID" \) -a \( "$1" = "wait" \) ]; then
   echo "waiting for litelinks process to exit after server shutdown triggered"
   while kill -0 ${LL_PID} 2> /dev/null; do sleep 1; done
   echo "litelinks server process exited"
fi
