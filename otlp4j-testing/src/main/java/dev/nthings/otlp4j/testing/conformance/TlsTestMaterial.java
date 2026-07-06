package dev.nthings.otlp4j.testing.conformance;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;
import java.util.List;
import java.util.regex.Pattern;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

/// Loads the checked-in test PEM material from the classpath and turns it into the trust and key
/// objects the wire clients need.
///
/// The five PEM files live under `dev/nthings/otlp4j/testing/conformance/tls/` and are read with
/// `getResourceAsStream`, so the harness needs no filesystem path and no build-time key
/// generation. The material is issued with a hundred-year validity by the committed `regen.sh`, so
/// it never rots. This class is the one place the byte layout of that material is understood.
public final class TlsTestMaterial {

    private static final String BASE = "/dev/nthings/otlp4j/testing/conformance/tls/";
    private static final Pattern PEM_KEY = Pattern.compile(
            "-----BEGIN (?:RSA )?PRIVATE KEY-----|-----END (?:RSA )?PRIVATE KEY-----|\\s");

    private TlsTestMaterial() {}

    /// The test CA certificate as PEM bytes. This is the trust anchor a server-auth client uses.
    ///
    /// @return the CA PEM bytes
    public static byte[] caPem() {
        return resource("ca.pem");
    }

    /// The server certificate as PEM bytes.
    ///
    /// @return the server certificate PEM bytes
    public static byte[] serverCertPem() {
        return resource("server-cert.pem");
    }

    /// The server private key as unencrypted PKCS8 PEM bytes.
    ///
    /// @return the server key PEM bytes
    public static byte[] serverKeyPem() {
        return resource("server-key.pem");
    }

    /// The client certificate as PEM bytes, presented in the mutual TLS scenario.
    ///
    /// @return the client certificate PEM bytes
    public static byte[] clientCertPem() {
        return resource("client-cert.pem");
    }

    /// The client private key as unencrypted PKCS8 PEM bytes.
    ///
    /// @return the client key PEM bytes
    public static byte[] clientKeyPem() {
        return resource("client-key.pem");
    }

    /// A fresh stream over the client certificate PEM, for a gRPC channel key manager.
    ///
    /// @return a new input stream over the client certificate
    public static InputStream clientCertStream() {
        return new ByteArrayInputStream(clientCertPem());
    }

    /// A fresh stream over the client key PEM, for a gRPC channel key manager.
    ///
    /// @return a new input stream over the client key
    public static InputStream clientKeyStream() {
        return new ByteArrayInputStream(clientKeyPem());
    }

    /// Builds a client-side [SSLContext] that trusts the given server trust PEM and, when
    /// `presentClientCert` is set, presents the checked-in client key material.
    ///
    /// @param trustPem the PEM bytes to trust as the server's CA
    /// @param presentClientCert whether to present the client certificate for a mutual scenario
    /// @return an SSL context configured for the requested posture
    public static SSLContext clientSslContext(byte[] trustPem, boolean presentClientCert) {
        try {
            var trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
            trustStore.load(null, null);
            var certs = parseCertificates(trustPem);
            for (var i = 0; i < certs.size(); i++) {
                trustStore.setCertificateEntry("ca-" + i, certs.get(i));
            }
            var tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(trustStore);

            KeyManagerFactory kmf = null;
            if (presentClientCert) {
                var keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
                keyStore.load(null, null);
                var chain = parseCertificates(clientCertPem()).toArray(new Certificate[0]);
                keyStore.setKeyEntry("client", parsePrivateKey(clientKeyPem()), new char[0], chain);
                kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                kmf.init(keyStore, new char[0]);
            }

            var context = SSLContext.getInstance("TLS");
            context.init(kmf == null ? null : kmf.getKeyManagers(), tmf.getTrustManagers(), null);
            return context;
        } catch (GeneralSecurityException | IOException e) {
            throw new IllegalStateException("failed to build client SSL context", e);
        }
    }

    private static List<X509Certificate> parseCertificates(byte[] pem) throws GeneralSecurityException {
        var factory = CertificateFactory.getInstance("X.509");
        var certs = new java.util.ArrayList<X509Certificate>();
        for (var cert : factory.generateCertificates(new ByteArrayInputStream(pem))) {
            certs.add((X509Certificate) cert);
        }
        return certs;
    }

    private static PrivateKey parsePrivateKey(byte[] pem) throws GeneralSecurityException {
        var base64 = PEM_KEY.matcher(new String(pem, java.nio.charset.StandardCharsets.US_ASCII))
                .replaceAll("");
        var der = Base64.getDecoder().decode(base64);
        return KeyFactory.getInstance("RSA").generatePrivate(new PKCS8EncodedKeySpec(der));
    }

    private static byte[] resource(String name) {
        try (InputStream in = TlsTestMaterial.class.getResourceAsStream(BASE + name)) {
            if (in == null) {
                throw new IllegalStateException("missing TLS test resource: " + BASE + name);
            }
            return in.readAllBytes();
        } catch (IOException e) {
            throw new UncheckedIOException("failed to read TLS test resource: " + BASE + name, e);
        }
    }
}
