#
# Copyright 2018 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

ARG CP_VERSION

FROM confluentinc/cp-kafka-connect:$CP_VERSION

ENV CONNECT_PLUGIN_PATH="/usr/share/java,/usr/share/confluent-hub-components"

ARG KAFKA_CONNECT_VERSION

COPY target/components/packages/confluentinc-kafka-connect-${KAFKA_CONNECT_VERSION}.zip /tmp/confluentinc-kafka-connect-${KAFKA_CONNECT_VERSION}.zip

RUN confluent-hub install --no-prompt /tmp/confluentinc-kafka-connect-${KAFKA_CONNECT_VERSION}.zip
