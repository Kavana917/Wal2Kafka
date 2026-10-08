FROM quay.io/debezium/connect:3.3

USER root

RUN microdnf install -y curl tar

RUN curl -L --fail \
    https://client.hub.confluent.io/confluent-hub-client-latest.tar.gz \
    -o /tmp/confluent-hub.tar.gz && \
    mkdir -p /opt/confluent-hub && \
    tar -xzf /tmp/confluent-hub.tar.gz \
    -C /opt/confluent-hub && \
    rm /tmp/confluent-hub.tar.gz

ENV PATH="/opt/confluent-hub/bin:${PATH}"

RUN mkdir -p /kafka/connect/kafka-connect-elasticsearch && \
    confluent-hub install \
    --no-prompt \
    --worker-configs /dev/null \
    --component-dir /kafka/connect/kafka-connect-elasticsearch \
    confluentinc/kafka-connect-elasticsearch:16.0.0

USER kafka