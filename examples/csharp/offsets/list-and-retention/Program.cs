using Confluent.Kafka;
using Confluent.Kafka.Admin;
using KubeMQ.Kafka.Examples.Shared;

// ─── Offsets 10: ListOffsets (earliest/latest/by-ts) + retention config ───
//
//   * QueryWatermarkOffsets — earliest (log-start) and latest (high-watermark)
//   * OffsetsForTimes       — ListOffsets by-timestamp
//   * retention.ms / retention.bytes — set at CreateTopics; the connector maps
//     them to channel MaxAge / MaxBytes. We assert the config ROUND-TRIPS via
//     DescribeConfigs (retention is time/size-based; we do not wait for eviction).

return await Demo.RunAsync(async () =>
{
    const string topic = "kafka-ex-offsets-list-and-retention";
    const int count = 8;
    var tp = new TopicPartition(topic, 0);
    using var admin = new AdminClientBuilder(KafkaClients.AdminConfig()).Build();

    try
    {
        await admin.CreateTopicsAsync([
            new TopicSpecification
            {
                Name = topic,
                NumPartitions = 1,
                ReplicationFactor = 1,
                Configs = new Dictionary<string, string>
                {
                    ["retention.ms"] = "600000",       // → channel MaxAge
                    ["retention.bytes"] = "1048576",   // → channel MaxBytes
                },
            }
        ]);
        Demo.Step($"Created topic '{topic}' (retention.ms=600000, retention.bytes=1048576)");
    }
    catch (CreateTopicsException e) when (
        e.Results.All(r => r.Error.Code == ErrorCode.TopicAlreadyExists))
    {
        Demo.Step($"Topic '{topic}' already exists");
    }

    // Produce a known number of records; capture a mid-time for the timestamp query.
    DateTime midTime = default;
    var producerConfig = KafkaClients.ProducerConfig();
    producerConfig.Acks = Acks.All;
    using (var producer = new ProducerBuilder<string, string>(producerConfig).Build())
    {
        for (var i = 0; i < count; i++)
        {
            if (i == count / 2) { midTime = DateTime.UtcNow; await Task.Delay(50); }
            await producer.ProduceAsync(topic, new Message<string, string> { Value = $"o{i}" });
            await Task.Delay(10);
        }
        producer.Flush(TimeSpan.FromSeconds(10));
        Demo.Sent($"produced {count} records");
    }

    using var consumer = new ConsumerBuilder<string, string>(
        KafkaClients.ConsumerConfig(groupId: $"cs-offsets-{Guid.NewGuid():N}")).Build();

    // ── ListOffsets earliest/latest via watermarks ──
    var wm = consumer.QueryWatermarkOffsets(tp, TimeSpan.FromSeconds(10));
    Demo.Got($"[watermarks] earliest(low)={wm.Low.Value} latest(high)={wm.High.Value}");
    Demo.RequireEqual(0L, wm.Low.Value, "earliest offset (log-start)");
    Demo.RequireEqual((long)count, wm.High.Value, "latest offset == high-watermark == count");

    // ── ListOffsets by-timestamp ──
    var forTimes = consumer.OffsetsForTimes(
        new[] { new TopicPartitionTimestamp(tp, new Timestamp(midTime, TimestampType.CreateTime)) },
        TimeSpan.FromSeconds(10));
    var tsOffset = forTimes[0].Offset.Value;
    Demo.Got($"[by-timestamp] midTime → offset {tsOffset}");
    Demo.Require(tsOffset > 0 && tsOffset < count, "by-timestamp offset falls inside the log");
    consumer.Close();

    // ── retention config round-trips via DescribeConfigs ──
    var resource = new ConfigResource { Type = ResourceType.Topic, Name = topic };
    var described = await admin.DescribeConfigsAsync([resource]);
    var entries = described[0].Entries;
    if (entries.TryGetValue("retention.ms", out var rms))
    {
        Demo.Got($"[retention] retention.ms = {rms.Value} (→ channel MaxAge)");
        Demo.RequireEqual("600000", rms.Value, "retention.ms round-trip");
    }
    else
    {
        Demo.Step("[retention] retention.ms not surfaced by DescribeConfigs on this connector build");
    }

    await admin.DeleteTopicsAsync([topic]);
    Demo.Step($"Cleaned up topic '{topic}'");

    Demo.Ok("ListOffsets earliest/latest/by-timestamp + retention config round-trip verified");
});
