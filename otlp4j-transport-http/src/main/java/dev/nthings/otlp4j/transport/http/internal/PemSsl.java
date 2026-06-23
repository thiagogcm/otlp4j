package dev.nthings.otlp4j.transport.http.internal;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

/// Builds JDK [SSLContext]s from PEM certificate/key files for the OTLP/HTTP transport, mirroring the
/// same PEM material the gRPC transport feeds to its native credentials. The HTTP transports use the
/// JDK [javax.net.ssl] stack (`java.net.http.HttpClient` and `com.sun.net.httpserver.HttpsServer`),
/// which take an [SSLContext] rather than PEM files directly.
///
/// The private key's algorithm is read from the paired certificate's public key, so RSA and EC keys
/// load without trial-and-error.
final class PemSsl {

    private static final char[] NO_PASSWORD = new char[0];

    private PemSsl() {}

    /// Server context: a key manager holding the certificate chain and its matching private key.
    static SSLContext serverContext(Path certFile, Path keyFile) {
        try {
            return context(keyManagers(certFile, keyFile), null);
        } catch (GeneralSecurityException | IOException e) {
            throw new IllegalArgumentException("failed to load server TLS material", e);
        }
    }

    /// Client context honouring a custom trust store and/or a client certificate (mTLS). A null
    /// `trustFile` keeps the JVM default trust; a null `certFile`/`keyFile` pair means no client
    /// certificate is presented.
    static SSLContext clientContext(Path certFile, Path keyFile, Path trustFile) {
        try {
            var trust = trustFile == null ? null : trustManagers(trustFile);
            var key = certFile == null ? null : keyManagers(certFile, keyFile);
            return context(key, trust);
        } catch (GeneralSecurityException | IOException e) {
            throw new IllegalArgumentException("failed to load client TLS material", e);
        }
    }

    private static SSLContext context(KeyManager[] keyManagers, TrustManager[] trustManagers)
            throws GeneralSecurityException {
        var ctx = SSLContext.getInstance("TLS");
        ctx.init(keyManagers, trustManagers, null);
        return ctx;
    }

    private static KeyManager[] keyManagers(Path certFile, Path keyFile)
            throws GeneralSecurityException, IOException {
        var chain = certificates(certFile);
        var key = privateKey(keyFile, chain.getFirst().getPublicKey().getAlgorithm());
        var ks = KeyStore.getInstance("PKCS12");
        ks.load(null, NO_PASSWORD);
        ks.setKeyEntry("key", key, NO_PASSWORD, chain.toArray(X509Certificate[]::new));
        var kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(ks, NO_PASSWORD);
        return kmf.getKeyManagers();
    }

    private static TrustManager[] trustManagers(Path trustFile)
            throws GeneralSecurityException, IOException {
        var ks = KeyStore.getInstance("PKCS12");
        ks.load(null, NO_PASSWORD);
        var certs = certificates(trustFile);
        for (var i = 0; i < certs.size(); i++) {
            ks.setCertificateEntry("ca-" + i, certs.get(i));
        }
        var tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(ks);
        return tmf.getTrustManagers();
    }

    private static List<X509Certificate> certificates(Path pem)
            throws GeneralSecurityException, IOException {
        var factory = CertificateFactory.getInstance("X.509");
        try (InputStream in = Files.newInputStream(pem)) {
            var certs = factory.generateCertificates(in);
            if (certs.isEmpty()) {
                throw new IllegalArgumentException("no certificates found in " + pem);
            }
            var list = new ArrayList<X509Certificate>(certs.size());
            for (var cert : certs) {
                list.add((X509Certificate) cert);
            }
            return list;
        }
    }

    private static PrivateKey privateKey(Path pem, String algorithm)
            throws GeneralSecurityException, IOException {
        var base64 = Files.readString(pem, StandardCharsets.UTF_8)
                .replaceAll("-----BEGIN [^-]+-----", "")
                .replaceAll("-----END [^-]+-----", "")
                .replaceAll("\\s", "");
        var der = Base64.getDecoder().decode(base64);
        return KeyFactory.getInstance(algorithm).generatePrivate(new PKCS8EncodedKeySpec(der));
    }
}
