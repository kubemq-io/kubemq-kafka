using Confluent.Kafka;
using Confluent.Kafka.Admin;
using KubeMQ.Kafka.Examples.Shared;

// ─── Consumer-groups 7: manual commit, resume, and lag ───
//
// EnableAutoCommit=false + explicit Commit(cr): consume HALF the records and
// commit, then STOP. A new consumer in the SAME group resumes from the committed
// offset (not from the start), proving OffsetCommit/OffsetFetch round-trips.
// Lag = high-watermark - committed offset; while records remain uncommitted the
// group reports lag (also surfaced connector-side as
// kubemq_kafka_consumer_group_lag{group,topic,partition}).

return await Demo.RunAsync(async () =>
{
    const string topic = "kafka-ex-consumer-groups-commit-and-lag";
    const int total = 10;
    const int firstBatch = 5;
    var groupId = $"cs-commit-lag-{Guid.NewGuid():N}";
    var tp = new TopicPartition(topic, 0);

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

    var producerConfig = KafkaClients.ProducerConfig();
    producerConfig.Acks = Acks.All;
    using (var producer = new ProducerBuilder<string, string>(producerConfig).Build())
    {
        for (var i = 0; i < total; i++)
            await producer.ProduceAsync(topic, new Message<string, string> { Value = $"job #{i}" });
        producer.Flush(TimeSpan.FromSeconds(10));
        Demo.Sent($"produced {total} records");
    }

    // ── Consumer A: consume the first half, commit, stop ──
    long committedOffset;
    using (var consumerA = new ConsumerBuilder<string, string>(
        KafkaClients.ConsumerConfig(groupId, AutoOffsetReset.Earliest, enableAutoCommit: false)).Build())
    {
        consumerA.Subscribe(topic);
        var consumed = 0;
        ConsumeResult<string, string>? last = null;
        var deadline = DateTime.UtcNow.AddSeconds(15);
        while (consumed < firstBatch && DateTime.UtcNow < deadline)
        {
            var cr = consumerA.Consume(TimeSpan.FromSeconds(2));
            if (cr is null) continue;
            last = cr;
            consumed++;
            Demo.Got($"[A] consumed '{cr.Message.Value}' at offset {cr.Offset.Value}");
        }
        Demo.RequireEqual(firstBatch, consumed, "consumer A must read the first batch");
        consumerA.Commit(last!);                    // commit offset = last.Offset + 1
        committedOffset = last!.Offset.Value + 1;
        Demo.Step($"[A] committed offset {committedOffset}");
        consumerA.Close();
    }

    // ── Lag: high-watermark - committed ──
    using (var admin = new AdminClientBuilder(KafkaClients.AdminConfig()).Build())
    {
        var listed = await admin.ListConsumerGroupOffsetsAsync(new[]
        {
            new ConsumerGroupTopicPartitions(groupId, [tp])
        });
        var committed = listed[0].Partitions[0].Offset.Value;
        Demo.RequireEqual(committedOffset, committed, "ListConsumerGroupOffsets committed offset");

        using var probe = new ConsumerBuilder<string, string>(
            KafkaClients.ConsumerConfig(groupId)).Build();
        var wm = probe.QueryWatermarkOffsets(tp, TimeSpan.FromSeconds(10));
        var lag = wm.High.Value - committed;
        Demo.Got($"[lag] highWatermark={wm.High.Value} committed={committed} → lag={lag}");
        Demo.RequireEqual((long)(total - firstBatch), lag, "reported lag = uncommitted records");
        probe.Close();
    }

    // ── Consumer B: same group resumes from the committed offset ──
    using (var consumerB = new ConsumerBuilder<string, string>(
        KafkaClients.ConsumerConfig(groupId, AutoOffsetReset.Earliest, enableAutoCommit: false)).Build())
    {
        consumerB.Subscribe(topic);
        var resumed = new List<string>();
        long? firstResumedOffset = null;
        var deadline = DateTime.UtcNow.AddSeconds(15);
        while (resumed.Count < (total - firstBatch) && DateTime.UtcNow < deadline)
        {
            var cr = consumerB.Consume(TimeSpan.FromSeconds(2));
            if (cr is null) continue;
            firstResumedOffset ??= cr.Offset.Value;
            resumed.Add(cr.Message.Value);
            Demo.Got($"[B] resumed '{cr.Message.Value}' at offset {cr.Offset.Value}");
        }
        consumerB.Close();
        Demo.RequireEqual(committedOffset, firstResumedOffset ?? -1,
            "consumer B resumes exactly at the committed offset");
        Demo.RequireEqual(total - firstBatch, resumed.Count, "consumer B drains the remainder");
        Demo.Require(!resumed.Any(v => v is "job #0" or "job #4"),
            "consumer B must not re-read committed records");
    }

    using (var admin = new AdminClientBuilder(KafkaClients.AdminConfig()).Build())
    {
        try { await admin.DeleteTopicsAsync([topic]); Demo.Step($"Cleaned up topic '{topic}'"); }
        catch (DeleteTopicsException) { /* best-effort */ }
    }

    Demo.Ok("Manual commit + resume-from-committed + lag all verified");
});
