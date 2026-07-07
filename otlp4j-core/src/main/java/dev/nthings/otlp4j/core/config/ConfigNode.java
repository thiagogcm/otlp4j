package dev.nthings.otlp4j.core.config;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/// An immutable tree over one parsed configuration sub-map, with typed, key-tracking accessors. A
/// declarative loader parses a document into nested maps, lists, and scalars, wraps the map for a
/// component in a `ConfigNode`, and hands it to that component's [ConfigBinder]. The node exposes the
/// values a factory reads, in the types it wants (string, int, long, boolean, duration, string list,
/// or a nested node), each keyed by name.
///
/// Every accessor records the key it touches, so [#unreadKeys] returns exactly the keys a binder never
/// read. That makes unknown-key diagnostics free: after a factory binds, the loader reports every
/// unread key as an unknown key on that component, without the factory maintaining a known-key list.
/// The wrapped map is copied defensively and never mutated, so the tree is immutable; only the private
/// read-tracking set changes as accessors run.
public final class ConfigNode {

    private static final ConfigNode EMPTY = new ConfigNode(Map.of());
    private static final Pattern DURATION_TOKEN = Pattern.compile("(\\d+)(ns|us|µs|ms|s|m|h)");

    private final Map<String, Object> raw;
    private final Set<String> read = new HashSet<>();

    private ConfigNode(Map<String, Object> raw) {
        // A LinkedHashMap preserves document order and, unlike Map.copyOf, tolerates the null values a
        // parser produces for a key with no value (`grpc:` with nothing after it).
        this.raw = Collections.unmodifiableMap(new LinkedHashMap<>(raw));
    }

    /// Wraps a parsed sub-map as a node. An empty map yields the shared empty node.
    ///
    /// @param raw the parsed configuration sub-map, keyed by name
    /// @return the node over the map
    public static ConfigNode of(Map<String, Object> raw) {
        if (raw == null) {
            throw new IllegalArgumentException("raw config map must be non-null; use ConfigNode.empty()");
        }
        return raw.isEmpty() ? EMPTY : new ConfigNode(raw);
    }

    /// The shared empty node, for a factory with no configuration.
    ///
    /// @return the empty node
    public static ConfigNode empty() {
        return EMPTY;
    }

    /// The string value under `key`, if present. A scalar value (string, number, or boolean) is
    /// rendered as its string form; a map or list value is not a scalar and yields empty.
    ///
    /// @param key the key to read
    /// @return the string value, or empty when absent or not scalar
    public Optional<String> string(String key) {
        var value = touch(key);
        if (value == null || value instanceof Map || value instanceof List) {
            return Optional.empty();
        }
        return Optional.of(String.valueOf(value));
    }

    /// The int value under `key`, or `def` when the key is absent.
    ///
    /// @param key the key to read
    /// @param def the default when the key is absent
    /// @return the int value, or the default
    public int intValue(String key, int def) {
        var value = touch(key);
        if (value == null) {
            return def;
        }
        if (value instanceof Number number) {
            return number.intValue();
        }
        try {
            return Integer.parseInt(String.valueOf(value).trim());
        } catch (NumberFormatException e) {
            throw malformed(key, "an int", value);
        }
    }

    /// The long value under `key`, or `def` when the key is absent.
    ///
    /// @param key the key to read
    /// @param def the default when the key is absent
    /// @return the long value, or the default
    public long longValue(String key, long def) {
        var value = touch(key);
        if (value == null) {
            return def;
        }
        if (value instanceof Number number) {
            return number.longValue();
        }
        try {
            return Long.parseLong(String.valueOf(value).trim());
        } catch (NumberFormatException e) {
            throw malformed(key, "a long", value);
        }
    }

    /// The boolean value under `key`, or `def` when the key is absent. A string value is accepted
    /// case-insensitively as `true` or `false`.
    ///
    /// @param key the key to read
    /// @param def the default when the key is absent
    /// @return the boolean value, or the default
    public boolean bool(String key, boolean def) {
        var value = touch(key);
        if (value == null) {
            return def;
        }
        if (value instanceof Boolean b) {
            return b;
        }
        var text = String.valueOf(value).trim();
        if (text.equalsIgnoreCase("true")) {
            return true;
        }
        if (text.equalsIgnoreCase("false")) {
            return false;
        }
        throw malformed(key, "a boolean", value);
    }

    /// The duration under `key`, or `def` when the key is absent. The value is a Collector-style
    /// duration string, one or more integer-plus-unit tokens (`ns`, `us`, `ms`, `s`, `m`, `h`) such as
    /// `1s`, `500ms`, or `1m30s`, or an ISO-8601 duration such as `PT1S`.
    ///
    /// @param key the key to read
    /// @param def the default when the key is absent
    /// @return the duration, or the default
    public Duration duration(String key, Duration def) {
        var value = touch(key);
        if (value == null) {
            return def;
        }
        return parseDuration(key, String.valueOf(value).trim());
    }

    /// The string list under `key`. A list value maps each element to its string form; a single scalar
    /// value yields a one-element list; an absent key yields an empty list.
    ///
    /// @param key the key to read
    /// @return the string list, never null
    public List<String> stringList(String key) {
        var value = touch(key);
        if (value == null) {
            return List.of();
        }
        if (value instanceof List<?> list) {
            return list.stream().map(String::valueOf).toList();
        }
        return List.of(String.valueOf(value));
    }

    /// The nested node under `key`, if the key is present. A map value yields a node over that map; a
    /// present but empty value (a key with nothing after it) yields the empty node; a scalar or list
    /// value yields empty, since it is not a sub-tree.
    ///
    /// @param key the key to read
    /// @return the nested node, or empty when the key is absent or not a sub-tree
    public Optional<ConfigNode> child(String key) {
        if (!raw.containsKey(key)) {
            return Optional.empty();
        }
        var value = touch(key);
        if (value == null) {
            return Optional.of(EMPTY);
        }
        if (value instanceof Map<?, ?> map) {
            @SuppressWarnings("unchecked")
            var typed = (Map<String, Object>) map;
            return Optional.of(of(typed));
        }
        return Optional.empty();
    }

    /// Every key present in this node, whether read or not. Reading this does not count as touching a
    /// key.
    ///
    /// @return the present keys
    public Set<String> keys() {
        return Set.copyOf(raw.keySet());
    }

    /// The keys present in this node that no accessor has touched, the basis for unknown-key
    /// diagnostics after a binder has run.
    ///
    /// @return the unread keys
    public Set<String> unreadKeys() {
        var unread = new HashSet<>(raw.keySet());
        unread.removeAll(read);
        return Collections.unmodifiableSet(unread);
    }

    private Object touch(String key) {
        read.add(key);
        return raw.get(key);
    }

    private static IllegalArgumentException malformed(String key, String expected, Object value) {
        return new IllegalArgumentException(
                "config key '" + key + "' is not " + expected + ": '" + value + "'");
    }

    private static Duration parseDuration(String key, String text) {
        if (text.isEmpty()) {
            throw malformed(key, "a duration", text);
        }
        var matcher = DURATION_TOKEN.matcher(text);
        var total = Duration.ZERO;
        var consumed = 0;
        var matchedAny = false;
        while (matcher.find() && matcher.start() == consumed) {
            total = total.plus(tokenDuration(matcher));
            consumed = matcher.end();
            matchedAny = true;
        }
        if (matchedAny && consumed == text.length()) {
            return total;
        }
        try {
            return Duration.parse(text);
        } catch (RuntimeException e) {
            throw malformed(key, "a duration", text);
        }
    }

    private static Duration tokenDuration(Matcher token) {
        var amount = Long.parseLong(token.group(1));
        return switch (token.group(2)) {
            case "ns" -> Duration.ofNanos(amount);
            case "us", "µs" -> Duration.ofNanos(amount * 1_000L);
            case "ms" -> Duration.ofMillis(amount);
            case "s" -> Duration.ofSeconds(amount);
            case "m" -> Duration.ofMinutes(amount);
            case "h" -> Duration.ofHours(amount);
            default -> throw new IllegalStateException("unreachable duration unit: " + token.group(2));
        };
    }
}
