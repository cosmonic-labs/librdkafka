#!/usr/bin/env bash
#
# Run one WASI test component under wasmtime using the settings written by
# start-kafka.sh.
#
#   usage: run-test.sh <component.wasm> [TOPIC] [GROUP]
#
# The wasmtime arguments are assembled into an array rather than interpolated
# from a string. A string would depend on the shell word-splitting it, which
# bash does and zsh does not — so a string form works in CI and silently breaks
# when someone reproduces the run locally.
#
set -euo pipefail

COMPONENT="${1:?usage: run-test.sh <component.wasm> [topic] [group]}"
TOPIC="${2:-}"
GROUP="${3:-}"

# Written by start-kafka.sh: BROKERS plus whatever the mode needs
# (SECURITY_PROTOCOL, SASL_*, SSL_CA_LOCATION, CERT_DIR).
# shellcheck disable=SC1091
source /tmp/kafka-env

args=(run -S inherit-network=y -S allow-ip-name-lookup=y)

# A component sees no ambient filesystem, so a CA file has to be granted
# explicitly as a preopen and referenced by its guest-side path.
if [ -n "${CERT_DIR:-}" ]; then
  args+=(--dir "${CERT_DIR}::/certs")
fi

args+=(--env "BROKERS=${BROKERS}")
[ -n "$TOPIC" ] && args+=(--env "TOPIC=${TOPIC}")
[ -n "$GROUP" ] && args+=(--env "GROUP=${GROUP}")

for v in SECURITY_PROTOCOL SASL_MECHANISM SASL_USERNAME SASL_PASSWORD SSL_CA_LOCATION; do
  if [ -n "${!v:-}" ]; then
    args+=(--env "${v}=${!v}")
  fi
done

args+=("$COMPONENT")

echo "+ wasmtime ${args[*]}"
exec wasmtime "${args[@]}"
