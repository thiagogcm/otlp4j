package dev.nthings.otlp4j.transport.http.internal;

import java.io.ByteArrayInputStream;
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

/// Builds JDK [SSLContext]s from PEM certificate/key material for the OTLP/HTTP transport, from
/// either files or in-memory bytes.
///
/// The HTTP transports use the JDK SSL stack which takes an [SSLContext] rather than PEM material
/// directly. The private key's algorithm is read from the paired certificate's public key, so RSA
/// and EC keys load without trial-and-error.
final class PemSsl {

    private static final char[] NO_PASSWORD = new char[0];

    private PemSsl() {}

    /// Server context with the certificate chain and matching private key file.
    static SSLContext serverContext(Path certFile, Path keyFile) {
        try {
            return context(keyManagers(certFile, keyFile), null);
        } catch (GeneralSecurityException | IOException e) {
            throw new IllegalArgumentException("failed to load server TLS material", e);
        }
    }

    /// Server context from in-memory PEM certificate chain and private key bytes.
    static SSLContext serverContext(byte[] cert, byte[] key) {
        try {
            return context(keyManagers(cert, key), null);
        } catch (GeneralSecurityException | IOException e) {
            throw new IllegalArgumentException("failed to load server TLS material", e);
        }
    }

    /// Client context with custom trust store and/or client certificate (mTLS). Null `trustFile`
    /// keeps the JVM default trust; null `certFile`/`keyFile` means no client certificate.
    static SSLContext clientContext(Path certFile, Path keyFile, Path trustFile) {
        try {
            var trust = trustFile == null ? null : trustManagers(certificates(trustFile));
            var key = certFile == null ? null : keyManagers(certFile, keyFile);
            return context(key, trust);
        } catch (GeneralSecurityException | IOException e) {
            throw new IllegalArgumentException("failed to load client TLS material", e);
        }
    }

    /// Client context from in-memory PEM bytes: the byte twin of [#clientContext(Path, Path, Path)].
    static SSLContext clientContext(byte[] cert, byte[] key, byte[] trust) {
        try {
            var trustMgrs = trust == null ? null : trustManagers(certificates(trust));
            var keyMgrs = cert == null ? null : keyManagers(cert, key);
            return context(keyMgrs, trustMgrs);
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
        return keyManagers(chain, privateKey(keyFile, algorithm(chain)));
    }

    private static KeyManager[] keyManagers(byte[] cert, byte[] key)
            throws GeneralSecurityException, IOException {
        var chain = certificates(cert);
        return keyManagers(chain, privateKey(key, algorithm(chain)));
    }

    private static KeyManager[] keyManagers(List<X509Certificate> chain, PrivateKey key)
            throws GeneralSecurityException, IOException {
        var ks = KeyStore.getInstance("PKCS12");
        ks.load(null, NO_PASSWORD);
        ks.setKeyEntry("key", key, NO_PASSWORD, chain.toArray(X509Certificate[]::new));
        var kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(ks, NO_PASSWORD);
        return kmf.getKeyManagers();
    }

    private static TrustManager[] trustManagers(List<X509Certificate> certs)
            throws GeneralSecurityException, IOException {
        var ks = KeyStore.getInstance("PKCS12");
        ks.load(null, NO_PASSWORD);
        for (var i = 0; i < certs.size(); i++) {
            ks.setCertificateEntry("ca-" + i, certs.get(i));
        }
        var tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(ks);
        return tmf.getTrustManagers();
    }

    private static String algorithm(List<X509Certificate> chain) {
        return chain.getFirst().getPublicKey().getAlgorithm();
    }

    private static List<X509Certificate> certificates(Path pem)
            throws GeneralSecurityException, IOException {
        try (InputStream in = Files.newInputStream(pem)) {
            return certificates(in);
        }
    }

    private static List<X509Certificate> certificates(byte[] pem)
            throws GeneralSecurityException, IOException {
        return certificates(new ByteArrayInputStream(pem));
    }

    private static List<X509Certificate> certificates(InputStream in)
            throws GeneralSecurityException, IOException {
        var factory = CertificateFactory.getInstance("X.509");
        var certs = factory.generateCertificates(in);
        if (certs.isEmpty()) {
            throw new IllegalArgumentException("no certificates found in PEM material");
        }
        var list = new ArrayList<X509Certificate>(certs.size());
        for (var cert : certs) {
            list.add((X509Certificate) cert);
        }
        return list;
    }

    private static PrivateKey privateKey(Path pem, String algorithm)
            throws GeneralSecurityException, IOException {
        return privateKey(Files.readString(pem, StandardCharsets.UTF_8), algorithm);
    }

    private static PrivateKey privateKey(byte[] pem, String algorithm) throws GeneralSecurityException {
        return privateKey(new String(pem, StandardCharsets.UTF_8), algorithm);
    }

    private static PrivateKey privateKey(String pem, String algorithm) throws GeneralSecurityException {
        var base64 = pem.replaceAll("-----BEGIN [^-]+-----", "")
                .replaceAll("-----END [^-]+-----", "")
                .replaceAll("\\s", "");
        var der = Base64.getDecoder().decode(base64);
        return KeyFactory.getInstance(algorithm).generatePrivate(new PKCS8EncodedKeySpec(der));
    }
}
