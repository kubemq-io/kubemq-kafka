package io.kubemq.examples.kafka.security.saslplainscram;

import io.kubemq.examples.kafka.shared.Check;
import io.kubemq.examples.kafka.shared.KafkaClients;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.TopicExistsException;

/**
 * security: sasl-plain-scram — SASL/PLAIN and SCRAM-SHA-256/512 authenticated
 * produce+consume, plus the denied path.
 *
 * <p>This variant is RUNNABLE only against a broker that has a Kafka credential store
 * (§4.7). Configure it with these env vars (SASL is off on a stock dev broker, so
 * without them the example prints the exact client config it WOULD use and exits 0):
 * <ul>
 *   <li>{@code KUBEMQ_KAFKA_SASL_MECHANISM} — {@code PLAIN} | {@code SCRAM-SHA-256} |
 *       {@code SCRAM-SHA-512}</li>
 *   <li>{@code KUBEMQ_KAFKA_SASL_USERNAME} / {@code KUBEMQ_KAFKA_SASL_PASSWORD}</li>
 *   <li>{@code KUBEMQ_KAFKA_SECURITY_PROTOCOL} — {@code SASL_PLAINTEXT} (default) or
 *       {@code SASL_SSL}</li>
 * </ul>
 *
 * <p>When configured: authenticated produce+consume must round-trip; then a bad-password
 * attempt must be denied — the client surfaces {@link SaslAuthenticationException}
 * (authentication) or {@link TopicAuthorizationException} (authorization), i.e. a
 * {@code *_AUTHORIZATION_FAILED} outcome. <b>TLS/mTLS is doc-only</b>: for encryption set
 * {@code security.protocol=SSL} (port 9093) with a truststore (mTLS adds a keystore) —
 * see {@code docs/guides/security-sasl-tls.md}. <b>gotcha #2:</b> external SASL/TLS also
 * needs {@code CONNECTORS_KAFKA_ADVERTISED_HOST} (and a cert SAN that matches it).
 */
public final class Main {

    private static final String TOPIC = "kafka-ex-security-sasl-java";

    private Main() {
    }

    public static void main(String[] args) throws Exception {
        System.out.println("bootstrap.servers = " + KafkaClients.bootstrap());

        String mechanism = env("KUBEMQ_KAFKA_SASL_MECHANISM", "PLAIN");
        String username = env("KUBEMQ_KAFKA_SASL_USERNAME", null);
        String password = env("KUBEMQ_KAFKA_SASL_PASSWORD", null);
        String securityProtocol = env("KUBEMQ_KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT");

        if (username == null || password == null) {
            // Doc-mode: no credential store wired up. Show the exact config that a
            // SASL-enabled broker would need, self-check it, and exit 0.
            System.out.println("SASL not configured (set KUBEMQ_KAFKA_SASL_USERNAME/"
                    + "KUBEMQ_KAFKA_SASL_PASSWORD to run live). Showing client config only.");
            for (String mech : List.of("PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512")) {
                String jaas = jaasConfig(mech, "demo-user", "demo-secret");
                Check.notBlank(jaas, "JAAS config string for " + mech);
                System.out.println("  " + mech + " -> sasl.jaas.config = " + jaas);
            }
            System.out.println("  TLS/mTLS is doc-only: security.protocol=SSL on :9093 "
                    + "(truststore; +keystore for mTLS) — see docs/guides/security-sasl-tls.md");
            System.out.println("OK: SASL/SCRAM client config emitted (doc-mode, no live broker)");
            return;
        }

        System.out.println("SASL live mode: mechanism=" + mechanism
                + " securityProtocol=" + securityProtocol + " user=" + username);

        // ---- Ensure the topic exists (with authenticated Admin). ----
        try (Admin admin = adminWithSasl(mechanism, securityProtocol, username, password)) {
            try {
                admin.createTopics(List.of(new NewTopic(TOPIC, 1, (short) 1))).all().get();
                System.out.println("CreateTopics '" + TOPIC + "' (1 partition)");
            } catch (Exception e) {
                if (e.getCause() instanceof TopicExistsException) {
                    System.out.println("Topic '" + TOPIC + "' already exists — reusing");
                } else {
                    throw e;
                }
            }
        }

        String run = Long.toString(System.currentTimeMillis());
        String value = "sasl-" + run;

        // ---- Authenticated produce + consume round-trip. ----
        Properties pp = KafkaClients.producerProps();
        pp.put(ProducerConfig.ACKS_CONFIG, "all");
        applySasl(pp, mechanism, securityProtocol, username, password);
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(pp)) {
            producer.send(new ProducerRecord<>(TOPIC, "k", value)).get();
            System.out.println("[auth] produced '" + value + "'");
        }

        Properties cp = KafkaClients.consumerProps(KafkaClients.freshGroup("security-sasl"));
        applySasl(cp, mechanism, securityProtocol, username, password);
        boolean roundTripped = false;
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(cp)) {
            consumer.subscribe(List.of(TOPIC));
            long deadline = System.currentTimeMillis() + 15_000;
            while (!roundTripped && System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> r : records) {
                    if (value.equals(r.value())) {
                        roundTripped = true;
                    }
                }
            }
        }
        Check.that(roundTripped, "authenticated produce/consume round-tripped");
        System.out.println("[auth] consumed '" + value + "'");

        // ---- Denied path: wrong password must fail authn/authz. ----
        Properties bad = KafkaClients.producerProps();
        bad.put(ProducerConfig.ACKS_CONFIG, "all");
        bad.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 10_000);
        applySasl(bad, mechanism, securityProtocol, username, password + "-wrong");
        boolean denied = false;
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(bad)) {
            producer.send(new ProducerRecord<>(TOPIC, "k", "should-fail")).get();
        } catch (Exception e) {
            Throwable cause = (e.getCause() != null) ? e.getCause() : e;
            denied = cause instanceof SaslAuthenticationException
                    || cause instanceof TopicAuthorizationException;
            System.out.println("Bad-cred produce denied -> " + cause.getClass().getSimpleName());
        }
        Check.that(denied, "bad credentials rejected (*_AUTHORIZATION_FAILED)");

        System.out.println("OK: SASL authenticated round-trip + denied bad-cred path");
    }

    private static Admin adminWithSasl(String mechanism, String securityProtocol,
            String username, String password) {
        Properties p = new Properties();
        p.put("bootstrap.servers", KafkaClients.bootstrap());
        applySasl(p, mechanism, securityProtocol, username, password);
        return Admin.create(p);
    }

    private static void applySasl(Properties p, String mechanism, String securityProtocol,
            String username, String password) {
        p.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);
        p.put(SaslConfigs.SASL_MECHANISM, mechanism);
        p.put(SaslConfigs.SASL_JAAS_CONFIG, jaasConfig(mechanism, username, password));
    }

    /** Builds the {@code sasl.jaas.config} inline string for the mechanism. */
    private static String jaasConfig(String mechanism, String username, String password) {
        String loginModule = mechanism.startsWith("SCRAM")
                ? "org.apache.kafka.common.security.scram.ScramLoginModule"
                : "org.apache.kafka.common.security.plain.PlainLoginModule";
        return loginModule + " required username=\"" + username
                + "\" password=\"" + password + "\";";
    }

    private static String env(String key, String fallback) {
        String v = System.getenv(key);
        return (v != null && !v.isBlank()) ? v : fallback;
    }
}
