using Confluent.Kafka;
using Confluent.Kafka.Admin;
using KubeMQ.Kafka.Examples.Shared;

// ─── Consume 4: auto.offset.reset earliest vs latest ───
//
// A fresh consumer group has no committed offset, so auto.offset.reset decides
// where it starts:
//   * Earliest — replays from the log start (sees pre-existing records)
//   * Latest   — only records produced AFTER the subscription is live
//
// To prove `latest`, we subscribe, wait until the consumer is assigned, THEN
// produce a marker; the latest consumer must see the marker but NONE of the
// pre-existing records.

return await Demo.RunAsync(async () =>
{
    const string topic = "kafka-ex-consume-from-beginning-latest";
    const int preExisting = 3;

    using (var admin = new AdminClientBuilder(KafkaClients.AdminConfig()).Build())
    {
        try
        {
            await admin.CreateTopicsAsync([
                new TopicSpecification { Name = topic, NumPartitions = 1, ReplicationFactor = 1 }
            ]);
            Demo.Step($"Created topic '{topic}'");
        }
        catch (CreateTopicsException e) when (
            e.Results.All(r => r.Error.Code == ErrorCode.TopicAlreadyExists))
        {
            Demo.Step($"Topic '{topic}' already exists");
        }
    }

    // Seed pre-existing records.
    var producerConfig = KafkaClients.ProducerConfig();
    producerConfig.Acks = Acks.All;
    using (var producer = new ProducerBuilder<string, string>(producerConfig).Build())
    {
        for (var i = 1; i <= preExisting; i++)
            await producer.ProduceAsync(topic, new Message<string, string> { Value = $"pre #{i}" });
        producer.Flush(TimeSpan.FromSeconds(10));
        Demo.Sent($"seeded {preExisting} pre-existing records");
    }

    // ── EARLIEST: a fresh group replays from the start ──
    var earliestConfig = KafkaClients.ConsumerConfig(
        groupId: $"cs-earliest-{Guid.NewGuid():N}", offsetReset: AutoOffsetReset.Earliest);
    using (var consumer = new ConsumerBuilder<string, string>(earliestConfig).Build())
    {
        consumer.Subscribe(topic);
        var seen = new List<string>();
        var deadline = DateTime.UtcNow.AddSeconds(15);
        while (seen.Count < preExisting && DateTime.UtcNow < deadline)
        {
            var cr = consumer.Consume(TimeSpan.FromSeconds(2));
            if (cr is null) continue;
            seen.Add(cr.Message.Value);
        }
        consumer.Close();
        Demo.Got($"[earliest] saw {seen.Count} record(s): {string.Join(", ", seen)}");
        Demo.RequireEqual(preExisting, seen.Count, "earliest consumer must see all pre-existing records");
    }

    // ── LATEST: subscribe, wait for assignment, then produce a marker ──
    var latestConfig = KafkaClients.ConsumerConfig(
        groupId: $"cs-latest-{Guid.NewGuid():N}", offsetReset: AutoOffsetReset.Latest);
    var assigned = new TaskCompletionSource();
    using (var consumer = new ConsumerBuilder<string, string>(latestConfig)
        .SetPartitionsAssignedHandler((_, parts) => assigned.TrySetResult())
        .Build())
    {
        consumer.Subscribe(topic);

        // Poll until the partitions-assigned handler fires (Consume drives callbacks).
        var assignDeadline = DateTime.UtcNow.AddSeconds(15);
        while (!assigned.Task.IsCompleted && DateTime.UtcNow < assignDeadline)
            consumer.Consume(TimeSpan.FromMilliseconds(200));
        Demo.Require(assigned.Task.IsCompleted, "latest consumer never got a partition assignment");
        Demo.Step("[latest] partitions assigned; producing marker now");

        const string marker = "MARKER-after-subscribe";
        using (var producer = new ProducerBuilder<string, string>(producerConfig).Build())
        {
            await producer.ProduceAsync(topic, new Message<string, string> { Value = marker });
            producer.Flush(TimeSpan.FromSeconds(10));
        }

        var seen = new List<string>();
        var deadline = DateTime.UtcNow.AddSeconds(15);
        while (DateTime.UtcNow < deadline)
        {
            var cr = consumer.Consume(TimeSpan.FromSeconds(2));
            if (cr is null)
            {
                if (seen.Contains(marker)) break;
                continue;
            }
            seen.Add(cr.Message.Value);
        }
        consumer.Close();
        Demo.Got($"[latest] saw {seen.Count} record(s): {string.Join(", ", seen)}");
        Demo.Require(seen.Contains(marker), "latest consumer must see the post-subscribe marker");
        Demo.Require(!seen.Any(v => v.StartsWith("pre #")),
            "latest consumer must NOT see pre-existing records");
    }

    using (var admin = new AdminClientBuilder(KafkaClients.AdminConfig()).Build())
    {
        try { await admin.DeleteTopicsAsync([topic]); Demo.Step($"Cleaned up topic '{topic}'"); }
        catch (DeleteTopicsException) { /* best-effort */ }
    }

    Demo.Ok("auto.offset.reset verified: earliest replays history, latest sees only new records");
});
