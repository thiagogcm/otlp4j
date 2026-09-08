# Conformance suite TLS material

These five PEM files are the certificate material the OTLP wire conformance suite uses for its TLS cases. They are loaded from the classpath at runtime by `TlsTestMaterial`, so the harness needs no filesystem path and no build-time key generation.

| File | What it is |
| --- | --- |
| `ca.pem` | Self-signed test CA certificate, the trust anchor a server-auth client verifies against. |
| `server-cert.pem` | Server certificate, `CN=localhost`, with SAN `DNS:localhost` and `IP:127.0.0.1`, signed by the CA. |
| `server-key.pem` | Server private key, unencrypted PKCS8, loaded directly by the servers. |
| `client-cert.pem` | Client certificate signed by the CA, presented in the mutual TLS scenario. |
| `client-key.pem` | Client private key, unencrypted PKCS8. |

The keys are unencrypted PKCS8 because `PemSsl.privateKey` reads them with a `PKCS8EncodedKeySpec` and the gRPC `TlsServerCredentials` builder loads the same format. The certificates are issued with a hundred-year validity so they never expire during the life of the project, which means the material never needs routine rotation.

## Regenerating

Run `regen.sh` directly so its `#!/usr/bin/env bash` shebang is honored, or use the wrapper recipe:

```
just conformance-certs
```

The script must run under bash, not a POSIX `/bin/sh` such as dash, because it uses process substitution (`<(...)`) for the server SAN extension. It regenerates the entire CA and both leaf certificates from scratch, then removes the transient CA key, serial, and CSR files so only the five committed PEMs remain. Regeneration is deterministic in structure but not in bytes: each run mints fresh random keys. Only regenerate when the test CA is deliberately rotated; a rotation changes the bytes of every file here.

The mutual TLS scenario in this resource covers optional client-certificate presentation. The current server authenticates itself but does not request or verify a client certificate, so `client-cert.pem` is presented by the client and ignored. Required mutual authentication is tested separately by the transport's required-mTLS suite.
