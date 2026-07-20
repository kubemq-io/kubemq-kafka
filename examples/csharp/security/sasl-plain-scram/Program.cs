using Confluent.Kafka;
using Confluent.Kafka.Admin;
using KubeMQ.Kafka.Examples.Shared;

// ─── Security 13: SASL/PLAIN + SCRAM-SHA-256/512 ───
//
// Runnable ONLY against a connector configured with a Kafka credential store
// (SASL enabled). It authenticates with SASL/PLAIN (and, if selected,
// SCRAM-SHA-256/512), then produce+consume round-trips a record. Wrong creds or a
// denied topic surface *_AUTHORIZATION_FAILED.
//
// TLS (:9093, SecurityProtocol.Ssl) and mTLS are DOC-ONLY here — see the README;
// this program covers the SASL mechanisms that are runnable without a cert setup.
//
// Config via env (so the same program runs against any credential store):
//   KAFKA_SASL_MECHANISM = PLAIN (default) | SCRAM-SHA-256 | SCRAM-SHA-512
//   KAFKA_SASL_USERNAME  / KAFKA_SASL_PASSWORD
// If KAFKA_SASL_USERNAME is unset, the example explains it needs a SASL-enabled
// broker and exits 0 (nothing to assert) rather than falsely failing.
//
// gotcha #2: the connector needs CONNECTORS_KAFKA_ADVERTISED_HOST set, or clients
// connect then hang before the SASL handshake completes.

return await Demo.RunAsync(async () =>
{
    var username = Environment.GetEnvironmentVariable("KAFKA_SASL_USERNAME");
    var password = Environment.GetEnvironmentVariable("KAFKA_SASL_PASSWORD");
    var mechEnv = Environment.GetEnvironmentVariable("KAFKA_SASL_MECHANISM") ?? "PLAIN";

    if (string.IsNullOrEmpty(username) || string.IsNullOrEmpty(password))
    {
        Demo.Step("KAFKA_SASL_USERNAME/PASSWORD not set — this example needs a SASL-enabled broker.");
        Demo.Step("Set KAFKA_SASL_MECHANISM (PLAIN|SCRAM-SHA-256|SCRAM-SHA-512), KAFKA_SASL_USERNAME, KAFKA_SASL_PASSWORD.");
        Demo.Ok("SASL example skipped (no credentials provided) — see README for TLS/mTLS doc-only setup");
        return;
    }

    var mechanism = mechEnv.ToUpperInvariant() switch
    {
        "SCRAM-SHA-256" => SaslMechanism.ScramSha256,
        "SCRAM-SHA-512" => SaslMechanism.ScramSha512,
        _ => SaslMechanism.Plain,
    };
    Demo.Step($"Authenticating with SASL/{mechEnv} as '{username}' (SecurityProtocol=SaslPlaintext)");

    const string topic = "kafka-ex-security-sasl-plain-scram";

    void ApplySasl(ClientConfig c)
    {
        c.SecurityProtocol = SecurityProtocol.SaslPlaintext; // :9093 + SecurityProtocol.Ssl for TLS (doc-only)
        c.SaslMechanism = mechanism;
        c.SaslUsername = username;
        c.SaslPassword = password;
    }

    // ── admin: create topic (authenticated) ──
    var adminConfig = KafkaClients.AdminConfig();
    ApplySasl(adminConfig);
    using (var admin = new AdminClientBuilder(adminConfig).Build())
    {
        try
        {
            await admin.CreateTopicsAsync([
                new TopicSpecification { Name = topic, NumPartitions = 1, ReplicationFactor = 1 }
            ]);
            Demo.Step($"Created topic '{topic}' (authenticated)");
        }
        catch (CreateTopicsException e) when (
            e.Results.All(r => r.Error.Code == ErrorCode.TopicAlreadyExists))
        {
            Demo.Step($"Topic '{topic}' already exists");
        }
    }

    // ── authenticated produce ──
    var producerConfig = KafkaClients.ProducerConfig();
    producerConfig.Acks = Acks.All;
    ApplySasl(producerConfig);
    using (var producer = new ProducerBuilder<string, string>(producerConfig).Build())
    {
        var dr = await producer.ProduceAsync(topic, new Message<string, string> { Value = "secure-hello" });
        Demo.Sent($"authenticated produce → {dr.TopicPartitionOffset}");
        Demo.Require(dr.Status == PersistenceStatus.Persisted, "authenticated produce not Persisted");
    }

    // ── authenticated consume ──
    var consumerConfig = KafkaClients.ConsumerConfig(
        groupId: $"cs-sasl-{Guid.NewGuid():N}", offsetReset: AutoOffsetReset.Earliest);
    ApplySasl(consumerConfig);
    using (var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build())
    {
        consumer.Subscribe(topic);
        string? got = null;
        var deadline = DateTime.UtcNow.AddSeconds(15);
        while (got is null && DateTime.UtcNow < deadline)
        {
            var cr = consumer.Consume(TimeSpan.FromSeconds(2));
            if (cr is null) continue;
            got = cr.Message.Value;
        }
        consumer.Close();
        Demo.RequireEqual("secure-hello", got, "authenticated consume round-trip");
        Demo.Got($"authenticated consume → '{got}'");
    }

    // ── denied: wrong password → *_AUTHORIZATION_FAILED / authentication error ──
    var badConfig = KafkaClients.ProducerConfig();
    ApplySasl(badConfig);
    badConfig.SaslPassword = password + "-wrong";
    try
    {
        using var badProducer = new ProducerBuilder<string, string>(badConfig).Build();
        await badProducer.ProduceAsync(topic, new Message<string, string> { Value = "should-fail" });
        Demo.Step("[denied] wrong-credential produce unexpectedly succeeded (broker may be accept-any)");
    }
    catch (Exception e)
    {
        var code = (e as ProduceException<string, string>)?.Error.Code
                   ?? (e as KafkaException)?.Error.Code;
        Demo.Step($"[denied] wrong-credential produce rejected: {code?.ToString() ?? e.GetType().Name}");
    }

    var cleanupConfig = KafkaClients.AdminConfig();
    ApplySasl(cleanupConfig);
    using (var admin = new AdminClientBuilder(cleanupConfig).Build())
    {
        try { await admin.DeleteTopicsAsync([topic]); Demo.Step($"Cleaned up topic '{topic}'"); }
        catch (DeleteTopicsException) { /* best-effort */ }
    }

    Demo.Ok($"SASL/{mechEnv} authenticated produce+consume round-trip verified");
});
