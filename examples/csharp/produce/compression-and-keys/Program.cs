using Confluent.Kafka;
using Confluent.Kafka.Admin;
using KubeMQ.Kafka.Examples.Shared;

// ─── Produce 3: Compression codecs + keyed partitioning ───
//
// Round-trips a record under every compression codec (None/Gzip/Snappy/Lz4/Zstd)
// and shows keyed partitioning: a fixed key always lands on the SAME partition.
//
// GOTCHA #4 (cross-client partitioning): Confluent.Kafka is librdkafka-based, so
// its DEFAULT partitioner is CRC32 (consistent_random) — the SAME group as
// Python/Ruby/Rust, but the OPPOSITE of Java/franz-go/kafkajs (murmur2). So the
// partition a key lands on here matches the CRC32 clients and DIFFERS from the
// Java/JS examples. Do NOT copy a murmur2 expected-partition. See
// docs/concepts/cross-client-partitioning.md.

return await Demo.RunAsync(async () =>
{
    const string topic = "kafka-ex-produce-compression-and-keys";
    const int partitions = 3;

    using (var admin = new AdminClientBuilder(KafkaClients.AdminConfig()).Build())
    {
        try
        {
            await admin.CreateTopicsAsync([
                new TopicSpecification { Name = topic, NumPartitions = partitions, ReplicationFactor = 1 }
            ]);
            Demo.Step($"Created topic '{topic}' with {partitions} partitions");
        }
        catch (CreateTopicsException e) when (
            e.Results.All(r => r.Error.Code == ErrorCode.TopicAlreadyExists))
        {
            Demo.Step($"Topic '{topic}' already exists");
        }
    }

    var codecs = new[]
    {
        CompressionType.None, CompressionType.Gzip, CompressionType.Snappy,
        CompressionType.Lz4, CompressionType.Zstd,
    };

    // ── Each codec round-trips: produce one keyed record per codec ──
    // Record the partition each key lands on to prove key→partition stability.
    var keyPartitions = new Dictionary<string, int>();
    foreach (var codec in codecs)
    {
        var config = KafkaClients.ProducerConfig();
        config.Acks = Acks.All;
        config.CompressionType = codec;
        using var producer = new ProducerBuilder<string, string>(config).Build();

        // Same key each time — the CRC32 partitioner must pick a stable partition.
        var key = "customer-42";
        var dr = await producer.ProduceAsync(topic, new Message<string, string>
        {
            Key = key,
            Value = $"payload compressed with {codec}",
        });
        Demo.Sent($"codec={codec,-6} key='{key}' → partition {dr.Partition.Value} @ offset {dr.Offset.Value}");
        Demo.Require(dr.Status == PersistenceStatus.Persisted, $"{codec} record not Persisted");

        if (keyPartitions.TryGetValue(key, out var prev))
            Demo.RequireEqual(prev, dr.Partition.Value, $"key '{key}' partition stable across codecs");
        else
            keyPartitions[key] = dr.Partition.Value;
    }

    // A different key may land on a different (CRC32-chosen) partition.
    var spreadConfig = KafkaClients.ProducerConfig();
    spreadConfig.Acks = Acks.All;
    var partitionsSeen = new HashSet<int>();
    using (var producer = new ProducerBuilder<string, string>(spreadConfig).Build())
    {
        foreach (var key in new[] { "alpha", "beta", "gamma", "delta", "epsilon" })
        {
            var dr = await producer.ProduceAsync(topic, new Message<string, string>
            {
                Key = key,
                Value = $"value for {key}",
            });
            partitionsSeen.Add(dr.Partition.Value);
            Demo.Got($"key='{key}' → CRC32 partition {dr.Partition.Value}");
        }
    }
    Demo.Require(partitionsSeen.Count >= 2, "keyed records should spread across >1 partition (CRC32)");

    // ── Read back and confirm every codec's value survived the round-trip ──
    var consumerConfig = KafkaClients.ConsumerConfig(
        groupId: $"cs-compression-{Guid.NewGuid():N}",
        offsetReset: AutoOffsetReset.Earliest);
    using (var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build())
    {
        consumer.Subscribe(topic);
        var values = new List<string>();
        var deadline = DateTime.UtcNow.AddSeconds(15);
        while (DateTime.UtcNow < deadline)
        {
            var cr = consumer.Consume(TimeSpan.FromSeconds(2));
            if (cr is null)
            {
                if (values.Count >= codecs.Length) break;
                continue;
            }
            values.Add(cr.Message.Value);
        }
        consumer.Close();
        foreach (var codec in codecs)
            Demo.Require(values.Contains($"payload compressed with {codec}"),
                $"{codec}-compressed record not read back");
    }

    using (var admin = new AdminClientBuilder(KafkaClients.AdminConfig()).Build())
    {
        try { await admin.DeleteTopicsAsync([topic]); Demo.Step($"Cleaned up topic '{topic}'"); }
        catch (DeleteTopicsException) { /* best-effort */ }
    }

    Demo.Ok("Compression codecs round-trip + CRC32 keyed partitioning verified (gotcha #4)");
});
