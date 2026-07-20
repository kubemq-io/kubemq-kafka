package io.kubemq.examples.kafka.shared;

import java.util.Objects;

/**
 * Tiny assertion helper so every example is a runnable proof, not a demo.
 *
 * <p>A failed assertion throws {@link AssertionError}; each {@code Main} lets it
 * propagate out of {@code main} so the JVM exits non-zero. The exec-maven-plugin
 * ({@code exec:exec}, forked JVM) surfaces that as a run failure. (Per
 * SHARED-CONVENTIONS: exit 0 on success, non-zero on any failed assertion or
 * unexpected error.)
 */
public final class Check {

    private Check() {
    }

    /** Fails (exits non-zero) when {@code condition} is false. */
    public static void that(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError("ASSERTION FAILED: " + message);
        }
    }

    /** Fails when {@code actual} does not equal {@code expected}. */
    public static void equal(Object expected, Object actual, String message) {
        if (!Objects.equals(expected, actual)) {
            throw new AssertionError(
                    "ASSERTION FAILED: " + message
                            + " (expected=" + expected + ", actual=" + actual + ")");
        }
    }

    /** Fails when {@code value} is null or blank. */
    public static void notBlank(String value, String message) {
        if (value == null || value.isBlank()) {
            throw new AssertionError("ASSERTION FAILED: " + message + " (was blank)");
        }
    }
}
