package dev.nthings.otlp4j.core.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.security.Principal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Unit tests for the auth value types: [AuthSources] and the sealed [AuthResult]. They validate their
/// components, copy their collections defensively, and expose the convenience constructors the
/// transport and the built-in authenticators use.
class AuthTypesTest {

    private static final Principal ALICE = () -> "alice";

    @Test
    @DisplayName("AuthSources copies headers defensively and rejects nulls")
    void authSourcesDefensiveCopy() {
        var headers = new java.util.LinkedHashMap<String, List<String>>();
        var values = new ArrayList<>(List.of("Bearer t"));
        headers.put("authorization", values);
        var sources = new AuthSources(headers, Optional.of(ALICE));

        values.add("mutated");
        headers.put("added", List.of("x"));
        assertThat(sources.headers()).containsOnlyKeys("authorization");
        assertThat(sources.headers().get("authorization")).containsExactly("Bearer t");
        assertThat(sources.peerCertificatePrincipal()).contains(ALICE);

        assertThatThrownBy(() -> new AuthSources(null, Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new AuthSources(Map.of(), null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("AuthSources.ofHeaders carries no peer certificate")
    void authSourcesOfHeaders() {
        var sources = AuthSources.ofHeaders(Map.of("x", List.of("1")));
        assertThat(sources.peerCertificatePrincipal()).isEmpty();
        assertThat(sources.headers()).containsOnlyKeys("x");
    }

    @Test
    @DisplayName("Authenticated carries a principal and defensively copied attributes")
    void authenticatedResult() {
        var attributes = new java.util.LinkedHashMap<String, List<String>>();
        attributes.put("role", new ArrayList<>(List.of("admin")));
        var result = new AuthResult.Authenticated(ALICE, attributes);

        attributes.clear();
        assertThat(result.principal()).isEqualTo(ALICE);
        assertThat(result.attributes().get("role")).containsExactly("admin");

        assertThatThrownBy(() -> new AuthResult.Authenticated(null, Map.of()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new AuthResult.Authenticated(ALICE, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("Denied requires a non-blank reason")
    void deniedResult() {
        assertThat(((AuthResult.Denied) AuthResult.denied("no token")).reason()).isEqualTo("no token");
        assertThatThrownBy(() -> new AuthResult.Denied(" "))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("the sealed result is exhaustively either authenticated or denied")
    void sealedResultSwitch() {
        assertThat(describe(AuthResult.authenticated(ALICE))).isEqualTo("ok:alice");
        assertThat(describe(AuthResult.denied("bad"))).isEqualTo("no:bad");
    }

    private static String describe(AuthResult result) {
        return switch (result) {
            case AuthResult.Authenticated a -> "ok:" + a.principal().getName();
            case AuthResult.Denied d -> "no:" + d.reason();
        };
    }
}
