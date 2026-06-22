package dev.nthings.otlp4j.model;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Covers the hand-rolled `byte[]` value semantics on [ProfilesData] and [ProfilesData.Profile] —
/// defensive copies on construction and access, and `Arrays`-based equals/hashCode — which the
/// record-generated versions don't provide and which are an easy locus to regress.
@DisplayName("ProfilesData byte[] record semantics")
class ProfilesDataRecordTest {

    private static ProfilesData.Profile profile(byte[] raw) {
        return new ProfilesData.Profile("0a0b", 1L, 2L, 3L, 4, 5, "pprof", raw);
    }

    @DisplayName("Profile clones rawProfile on construction — caller mutation does not leak in")
    @Test
    void profileClonesRawProfileOnConstruction() {
        var raw = new byte[] {1, 2, 3};
        var p = profile(raw);
        raw[0] = 99;
        assertThat(p.rawProfile()).isEqualTo(new byte[] {1, 2, 3});
    }

    @DisplayName("Profile clones rawProfile on access — returned-array mutation does not corrupt state")
    @Test
    void profileClonesRawProfileOnAccess() {
        var p = profile(new byte[] {1, 2, 3});
        p.rawProfile()[0] = 99;
        assertThat(p.rawProfile()).isEqualTo(new byte[] {1, 2, 3});
    }

    @DisplayName("Profile equals/hashCode use array value semantics")
    @Test
    void profileEqualityIsByValue() {
        var a = profile(new byte[] {1, 2, 3});
        var b = profile(new byte[] {1, 2, 3});
        var c = profile(new byte[] {1, 2, 4});
        assertThat(a).isEqualTo(b).hasSameHashCodeAs(b);
        assertThat(a).isNotEqualTo(c);
    }

    @DisplayName("Profile equals/hashCode/toString tolerate null string fields")
    @Test
    void profileToleratesNullStrings() {
        var a = new ProfilesData.Profile(null, 1L, 2L, 3L, 4, 5, null, new byte[0]);
        var b = new ProfilesData.Profile(null, 1L, 2L, 3L, 4, 5, null, new byte[0]);
        assertThat(a).isEqualTo(b).hasSameHashCodeAs(b);
        assertThat(a.toString()).isNotNull();
    }

    @DisplayName("ProfilesData clones the dictionary on construction and access")
    @Test
    void profilesDataClonesDictionary() {
        var dict = new byte[] {7, 8, 9};
        var data = new ProfilesData(List.of(), dict);
        dict[0] = 0;
        assertThat(data.dictionary()).isEqualTo(new byte[] {7, 8, 9});
        data.dictionary()[1] = 0;
        assertThat(data.dictionary()).isEqualTo(new byte[] {7, 8, 9});
    }

    @DisplayName("ProfilesData equals/hashCode use dictionary value semantics")
    @Test
    void profilesDataEqualityIsByValue() {
        var a = new ProfilesData(List.of(), new byte[] {1});
        var b = new ProfilesData(List.of(), new byte[] {1});
        var c = new ProfilesData(List.of(), new byte[] {2});
        assertThat(a).isEqualTo(b).hasSameHashCodeAs(b);
        assertThat(a).isNotEqualTo(c);
    }
}
