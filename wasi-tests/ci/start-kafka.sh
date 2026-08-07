#!/usr/bin/env bash
#
# Start a single-node Kafka (KRaft) configured for one of the WASI test modes
# and write the client settings the test harness needs to /tmp/kafka-env.
#
#   usage: start-kafka.sh <plaintext|sasl|tls> [image]
#
# Kept out of the workflow so the same broker can be reproduced locally:
#   bash wasi/ci/start-kafka.sh tls && source /tmp/kafka-env
#
set -euo pipefail

MODE="${1:-plaintext}"
IMAGE="${2:-apache/kafka:4.1.0}"
SECRETS="${SECRETS_DIR:-/tmp/kafka-secrets}"

common_env=(
  -e KAFKA_NODE_ID=1
  -e KAFKA_PROCESS_ROLES=broker,controller
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9094
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1
  # Rebalance immediately; the default 3s delay just adds latency per job.
  -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0
)

# Every mode exposes a container-internal PLAINTEXT listener on 9095 purely so
# the admin tools (readiness probe, topic creation, broker-side verification)
# have an unauthenticated way in. It is never published to the host, so it does
# not weaken what the test actually exercises — the client always goes through
# the mode's real listener.
ADMIN="127.0.0.1:9095"

docker rm -f kafka >/dev/null 2>&1 || true
rm -rf "$SECRETS"; mkdir -p "$SECRETS"

case "$MODE" in
  plaintext)
    docker run -d --name kafka -p 9092:9092 "${common_env[@]}" \
      -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,ADMIN://0.0.0.0:9095,CONTROLLER://0.0.0.0:9094 \
      -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:9092,ADMIN://127.0.0.1:9095 \
      -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,ADMIN:PLAINTEXT,CONTROLLER:PLAINTEXT \
      -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
      "$IMAGE" >/dev/null
    cat > /tmp/kafka-env <<'EOF'
BROKERS=127.0.0.1:9092
EOF
    ;;

  sasl)
    # The image's configure script mangles an inline JAAS string (it performs
    # indirect expansion on it), so pass a mounted file instead.
    cat > "$SECRETS/jaas.conf" <<'EOF'
KafkaServer {
  org.apache.kafka.common.security.plain.PlainLoginModule required
  username="admin"
  password="admin-secret"
  user_admin="admin-secret"
  user_wasm="wasm-secret";
};
EOF
    # Inter-broker stays on the plain ADMIN listener so the broker does not have
    # to authenticate to itself; the SASL listener is what the client uses.
    docker run -d --name kafka -p 9092:9092 \
      -v "$SECRETS":/etc/kafka/jaas:ro "${common_env[@]}" \
      -e KAFKA_LISTENERS=SASL_PLAINTEXT://0.0.0.0:9092,ADMIN://0.0.0.0:9095,CONTROLLER://0.0.0.0:9094 \
      -e KAFKA_ADVERTISED_LISTENERS=SASL_PLAINTEXT://127.0.0.1:9092,ADMIN://127.0.0.1:9095 \
      -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=SASL_PLAINTEXT:SASL_PLAINTEXT,ADMIN:PLAINTEXT,CONTROLLER:PLAINTEXT \
      -e KAFKA_INTER_BROKER_LISTENER_NAME=ADMIN \
      -e KAFKA_SASL_ENABLED_MECHANISMS=PLAIN \
      -e KAFKA_OPTS="-Djava.security.auth.login.config=/etc/kafka/jaas/jaas.conf" \
      "$IMAGE" >/dev/null
    cat > /tmp/kafka-env <<'EOF'
BROKERS=127.0.0.1:9092
SECURITY_PROTOCOL=SASL_PLAINTEXT
SASL_MECHANISM=PLAIN
SASL_USERNAME=wasm
SASL_PASSWORD=wasm-secret
EOF
    ;;

  tls)
    keytool -genkeypair -alias kafka -keyalg RSA -keysize 2048 -validity 3650 \
      -dname "CN=localhost,OU=ci,O=ci,L=ci,S=ci,C=US" \
      -ext "SAN=DNS:localhost,IP:127.0.0.1" \
      -keystore "$SECRETS/server.keystore.jks" \
      -storepass changeit -keypass changeit >/dev/null
    keytool -exportcert -alias kafka -keystore "$SECRETS/server.keystore.jks" \
      -storepass changeit -rfc -file "$SECRETS/ca.pem" >/dev/null
    keytool -importcert -alias kafka -file "$SECRETS/ca.pem" \
      -keystore "$SECRETS/server.truststore.jks" \
      -storepass changeit -noprompt >/dev/null
    echo "changeit" > "$SECRETS/creds"

    # Inter-broker stays plain deliberately. With SSL inter-broker the broker
    # validates its own client/server SSLEngine pair at startup and a
    # self-signed cert fails PKIX there; the SSL *client* listener is what the
    # test actually exercises.
    docker run -d --name kafka -p 9093:9093 \
      -v "$SECRETS":/etc/kafka/secrets:ro "${common_env[@]}" \
      -e KAFKA_LISTENERS=SSL://0.0.0.0:9093,ADMIN://0.0.0.0:9095,CONTROLLER://0.0.0.0:9094 \
      -e KAFKA_ADVERTISED_LISTENERS=SSL://127.0.0.1:9093,ADMIN://127.0.0.1:9095 \
      -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=SSL:SSL,ADMIN:PLAINTEXT,CONTROLLER:PLAINTEXT \
      -e KAFKA_INTER_BROKER_LISTENER_NAME=ADMIN \
      -e KAFKA_SSL_KEYSTORE_FILENAME=server.keystore.jks \
      -e KAFKA_SSL_KEYSTORE_CREDENTIALS=creds \
      -e KAFKA_SSL_KEY_CREDENTIALS=creds \
      -e KAFKA_SSL_KEYSTORE_TYPE=PKCS12 \
      -e KAFKA_SSL_TRUSTSTORE_FILENAME=server.truststore.jks \
      -e KAFKA_SSL_TRUSTSTORE_CREDENTIALS=creds \
      -e KAFKA_SSL_TRUSTSTORE_TYPE=PKCS12 \
      -e KAFKA_SSL_CLIENT_AUTH=none \
      "$IMAGE" >/dev/null
    # A component has no ambient filesystem, so the CA must arrive through a
    # preopen and be referenced by its guest-side path.
    cat > /tmp/kafka-env <<EOF
BROKERS=127.0.0.1:9093
SECURITY_PROTOCOL=SSL
SSL_CA_LOCATION=/certs/ca.pem
CERT_DIR=$SECRETS
EOF
    ;;

  *)
    echo "unknown mode: $MODE" >&2
    exit 2
    ;;
esac

# Readiness probe is topic creation itself, not `--list`. `--list` succeeds
# against a broker that is still coming up (it happily returns an empty set),
# which reports ready within a second and then flakes on the first real call.
echo "waiting for broker (mode=$MODE)..."
ready=0
for i in $(seq 1 90); do
  if docker exec kafka /opt/kafka/bin/kafka-topics.sh \
       --bootstrap-server "$ADMIN" \
       --create --if-not-exists --topic wasi-producer-topic \
       --partitions 1 --replication-factor 1 >/dev/null 2>&1; then
    ready=1
    echo "broker ready after ${i}s"
    break
  fi
  if ! docker ps --format '{{.Names}}' | grep -qx kafka; then
    echo "broker container exited:" >&2
    docker logs kafka 2>&1 | tail -30 >&2
    exit 1
  fi
  sleep 1
done

if [ "$ready" -ne 1 ]; then
  echo "broker did not become ready" >&2
  docker logs kafka 2>&1 | tail -30 >&2
  exit 1
fi

docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "$ADMIN" \
  --create --if-not-exists --topic wasi-consumer-topic \
  --partitions 2 --replication-factor 1 >/dev/null

echo "--- client settings ---"
cat /tmp/kafka-env
