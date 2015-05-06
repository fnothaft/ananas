#!/usr/bin/env bash
#
# Copyright 2015 Frank Austin Nothaft
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
#

# figure out where ANANAS is installed
ANANAS_REPO="$(cd `dirname $0`/..; pwd)"

# Next three commands set CLASSPATH like appassembler
BASEDIR="$ANANAS_REPO"/target/appassembler
REPO="$BASEDIR"/repo
if [ ! -f "$BASEDIR"/bin/ananas ]; then
  echo "Failed to find appassembler scripts in $BASEDIR/bin"
  echo "You need to build RNAdam before running this program"
  exit 1
fi
eval $(cat "$BASEDIR"/bin/ananas | grep "^CLASSPATH")

echo "$CLASSPATH"
