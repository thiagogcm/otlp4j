# TLS test fixtures

The wire conformance suites load these PEM fixtures from the classpath through `TlsTestMaterial`. Tests need no external certificate files or build-time key generation. This material is for tests.

| File | Purpose |
| --- | --- |
| `ca.pem` | Self-signed test CA and client trust anchor |
| `server-cert.pem` | CA-signed localhost server certificate with `DNS:localhost` and `IP:127.0.0.1` subject alternative names |
| `server-key.pem` | Unencrypted PKCS8 server private key |
| `client-cert.pem` | CA-signed certificate for client-certificate scenarios |
| `client-key.pem` | Unencrypted PKCS8 client private key |

The regeneration script uses long certificate validity to reduce routine expiry in local tests. PKCS8 keys match the fixture loader and TLS credential APIs.

## Certificate-authentication coverage

| Scenario | What it checks |
| --- | --- |
| Shared server-authentication suite | Trusted server succeeds; untrusted server fails |
| Shared client-certificate presentation scenario | A client with credentials can connect to a server configured with `ClientAuth.NONE`; this does not verify mutual authentication |
| Dedicated required-mTLS suites | A trusted client certificate succeeds; a client without one is rejected |

The fixture files can support both presentation and required authentication. Receiver policy, not the presence of a client PEM file alone, determines whether the server verifies a client. See [TLS and client authentication](../../../../../../../../../../docs/transport.md#tls-and-client-authentication).

## Regenerate the fixture set

From the repository root, run:

```sh
just conformance-certs
```

Alternatively, execute this directory's `regen.sh` directly so its Bash shebang is honored. The script uses process substitution and requires Bash and OpenSSL.

Regeneration creates fresh random keys, a new CA, and both leaf certificates. It replaces the full fixture set and removes temporary CA-key, serial, and CSR files. Regenerate the set together when rotating the test CA or changing certificate properties, then rerun the affected TLS suites.
