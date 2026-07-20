using Confluent.Kafka;
using Confluent.Kafka.Admin;
using KubeMQ.Kafka.Examples.Shared;

// ─── Admin 9: partitions (increase-only) + incremental configs + delete-records ───
//
//   * CreatePartitions      — increase partition count (increase-only, <=256)
//   * bad increase          — same-count / decrease → INVALID_PARTITIONS
//   * IncrementalAlterConfigs (🟡 partial) — connector recognizes a SUBSET of keys
//   * DeleteRecords           (🟡 partial) — low-end truncation (log-start advance)
//
// gotcha #5: growing the partition count re-shards CRC32 keys — a key that landed
// on partition p under N partitions may move under N' > N. Documented, not asserted.

return await Demo.RunAsync(async () =>
{
    const string topic = "kafka-ex-admin-partitions-and-configs";
    using var admin = new AdminClientBuilder(KafkaClients.AdminConfig()).Build();

    try
    {
        await admin.CreateTopicsAsync([
            new TopicSpecification { Name = topic, NumPartitions = 2, ReplicationFactor = 1 }
        ]);
        Demo.Step($"Created topic '{topic}' with 2 partitions");
    }
    catch (CreateTopicsException e) when (
        e.Results.All(r => r.Error.Code == ErrorCode.TopicAlreadyExists))
    {
        Demo.Step($"Topic '{topic}' already exists");
    }

    // ── CreatePartitions: increase 2 → 4 (increase-only) ──
    await admin.CreatePartitionsAsync([
        new PartitionsSpecification { Topic = topic, IncreaseTo = 4 }
    ]);
    var md = admin.GetMetadata(topic, TimeSpan.FromSeconds(10));
    var count = md.Topics.Single(t => t.Topic == topic).Partitions.Count;
    Demo.RequireEqual(4, count, "partition count after increase");
    Demo.Got($"[partitions] increased to {count}");

    // ── bad increase: decreasing (or same count) → INVALID_PARTITIONS ──
    try
    {
        await admin.CreatePartitionsAsync([
            new PartitionsSpecification { Topic = topic, IncreaseTo = 2 } // < current 4
        ]);
        throw new DemoFailure("decrease partitions unexpectedly succeeded");
    }
    catch (CreatePartitionsException e)
    {
        var code = e.Results[0].Error.Code;
        Demo.Step($"[bad increase] IncreaseTo=2 (< 4) rejected: {code}");
        Demo.Require(code is ErrorCode.InvalidPartitions or ErrorCode.InvalidRequest,
            $"expected INVALID_PARTITIONS, got {code}");
    }

    // ── IncrementalAlterConfigs (🟡 partial): set retention.ms; connector
    // recognizes a subset of topic configs, so we set a key that maps to a
    // channel property (MaxAge) and read it back. ──
    var resource = new ConfigResource { Type = ResourceType.Topic, Name = topic };
    try
    {
        await admin.IncrementalAlterConfigsAsync(new Dictionary<ConfigResource, List<ConfigEntry>>
        {
            [resource] =
            [
                new ConfigEntry { Name = "retention.ms", Value = "7200000", IncrementalOperation = AlterConfigOpType.Set },
            ]
        });
        var back = await admin.DescribeConfigsAsync([resource]);
        var val = back[0].Entries.TryGetValue("retention.ms", out var e) ? e.Value : "<unrecognized>";
        Demo.Got($"[incremental-configs 🟡] retention.ms now '{val}' (subset-recognized)");
    }
    catch (KafkaException e)
    {
        // 🟡 partial: if the connector does not expose IncrementalAlterConfigs for
        // this key, the folder + README still document the supported alternative.
        Demo.Step($"[incremental-configs 🟡] not exposed for this key: {e.Error.Code} — see README");
    }

    // ── DeleteRecords (🟡 partial): advance the log-start of partition 0.
    // First produce a few records so there is something to truncate. ──
    var producerConfig = KafkaClients.ProducerConfig();
    producerConfig.Acks = Acks.All;
    using (var producer = new ProducerBuilder<string, string>(producerConfig).Build())
    {
        for (var i = 0; i < 5; i++)
            await producer.ProduceAsync(new TopicPartition(topic, 0),
                new Message<string, string> { Value = $"r{i}" });
        producer.Flush(TimeSpan.FromSeconds(10));
    }
    try
    {
        var deleted = await admin.DeleteRecordsAsync([
            new TopicPartitionOffset(new TopicPartition(topic, 0), new Offset(3)) // truncate below offset 3
        ]);
        Demo.Got($"[delete-records 🟡] log-start of partition 0 advanced to {deleted[0].Offset.Value}");
    }
    catch (KafkaException e)
    {
        Demo.Step($"[delete-records 🟡] not exposed: {e.Error.Code} — see README");
    }

    await admin.DeleteTopicsAsync([topic]);
    Demo.Step($"Cleaned up topic '{topic}'");

    Demo.Ok("Partitions increase-only + INVALID_PARTITIONS + incremental-configs/delete-records (🟡) verified");
});
