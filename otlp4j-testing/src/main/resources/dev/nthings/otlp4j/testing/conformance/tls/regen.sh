#!/usr/bin/env bash
# Regenerates the checked-in TLS test material for the conformance suite. The output is five PEM
# files: a self-signed test CA, a server certificate and key, and a client certificate and key.
# The certificates are issued with a hundred-year validity so they never rot; regeneration is only
# needed if the test CA is deliberately rotated.
#
# This script uses bash process substitution for the SAN extension, so it must run under bash and
# never under a POSIX /bin/sh such as dash, where <(...) is a syntax error. Invoke it directly by
# path so its shebang is honored (see the `conformance-certs` just recipe), not as `sh regen.sh`.
set -euo pipefail

# Operate in this script's own directory so the PEMs land beside it regardless of the caller's cwd.
cd "$(dirname "$0")"

# Self-signed test CA.
openssl req -x509 -newkey rsa:2048 -nodes -keyout ca-key.pem -out ca.pem \
  -days 36500 -subj "/CN=otlp4j-test-ca"

# Server key and certificate, signed by the CA, with SAN localhost and 127.0.0.1.
openssl req -newkey rsa:2048 -nodes -keyout server-key.raw.pem -out server.csr \
  -subj "/CN=localhost"
openssl x509 -req -in server.csr -CA ca.pem -CAkey ca-key.pem -CAcreateserial \
  -days 36500 -out server-cert.pem \
  -extfile <(printf "subjectAltName=DNS:localhost,IP:127.0.0.1")

# Client key and certificate, signed by the CA, for the mutual TLS scenario.
openssl req -newkey rsa:2048 -nodes -keyout client-key.raw.pem -out client.csr \
  -subj "/CN=otlp4j-test-client"
openssl x509 -req -in client.csr -CA ca.pem -CAkey ca-key.pem -CAcreateserial \
  -days 36500 -out client-cert.pem

# Emit the final keys as unencrypted PKCS8, which is what PemSsl.privateKey (PKCS8EncodedKeySpec)
# and TlsServerCredentials load directly.
openssl pkcs8 -topk8 -nocrypt -in server-key.raw.pem -out server-key.pem
openssl pkcs8 -topk8 -nocrypt -in client-key.raw.pem -out client-key.pem

# Keep only the committed material: the five PEMs, this script, and the README.
rm -f ca-key.pem ca.srl server.csr client.csr server-key.raw.pem client-key.raw.pem
