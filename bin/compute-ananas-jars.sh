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

# Figure out where ananas is installed
ANANAS_REPO="$(cd `dirname $0`/..; pwd)"

CLASSPATH=$("$ANANAS_REPO"/bin/compute-ananas-classpath.sh)

# list of jars to ship with spark; trim off the first and last from the CLASSPATH
# TODO: brittle? assumes appassembler always puts the $BASE/etc first and the CLI jar last
ANANAS_JARS=$(echo "$CLASSPATH" | tr ":" "," | cut -d "," -f 2- | rev | cut -d "," -f 2- | rev)

echo "$ANANAS_JARS"
