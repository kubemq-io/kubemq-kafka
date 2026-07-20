using Confluent.Kafka;
using Confluent.Kafka.Admin;
using KubeMQ.Kafka.Examples.Shared;

// ─── Consume 5: seek by offset + lookup by timestamp ───
//
// Uses manual Assign (not Subscribe) so Seek is valid on a fixed partition.
//   * Seek(offset)     — re-position to an exact offset and read that record
//   * OffsetsForTimes  — ListOffsets by-timestamp: given a wall-clock time,
//                        return the first offset whose record timestamp >= it
//
// We produce records with a small gap so a mid-run timestamp maps to a known
// record boundary.

return await Demo.RunAsync(async () =>
{
    // Per-run-unique topic (§4.2, matching the other languages): DeleteTopics does
    // NOT purge the connector channel — a re-created same-name topic keeps advancing
    // the offset sequence, so the log-start moves past 0 on a rerun. This example
    // Seeks to ABSOLUTE offsets, which would fall below the advanced log-start and
    // reset out-of-range on a second run against a fixed name. A fresh Guid channel
    // always starts at offset 0.
    var topic = $"kafka-ex-consume-seek-offsets-timestamps-{Guid.NewGuid():N}";
    const int count = 6;
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

    // Produce with a gap so timestamps are distinguishable. Capture the wall-clock
    // time just before producing record index `midIndex`.
    const int midIndex = 3;
    DateTime midTime = default;
    var producerConfig = KafkaClients.ProducerConfig();
    producerConfig.Acks = Acks.All;
    using (var producer = new ProducerBuilder<string, string>(producerConfig).Build())
    {
        for (var i = 0; i < count; i++)
        {
            if (i == midIndex)
            {
                midTime = DateTime.UtcNow;
                await Task.Delay(50);
            }
            await producer.ProduceAsync(topic, new Message<string, string>
            {
                Value = $"record #{i}",
                Timestamp = new Timestamp(DateTime.UtcNow, TimestampType.CreateTime),
            });
            await Task.Delay(20);
        }
        producer.Flush(TimeSpan.FromSeconds(10));
        Demo.Sent($"produced {count} records; captured mid-time before record #{midIndex}");
    }

    using var consumer = new ConsumerBuilder<string, string>(
        KafkaClients.ConsumerConfig(groupId: $"cs-seek-{Guid.NewGuid():N}")).Build();

    // Manual assignment — required for Seek to be valid.
    consumer.Assign(new TopicPartitionOffset(tp, Offset.Beginning));

    // ── Seek by offset: jump straight to offset 4 and read it ──
    const long seekTo = 4;
    consumer.Seek(new TopicPartitionOffset(tp, new Offset(seekTo)));
    var seekResult = ConsumeOne(consumer, TimeSpan.FromSeconds(10));
    Demo.Require(seekResult is not null, "seek(offset) read nothing");
    Demo.Got($"[seek] offset {seekTo} → '{seekResult!.Message.Value}' at offset {seekResult.Offset.Value}");
    Demo.RequireEqual(seekTo, seekResult.Offset.Value, "seek landed on the requested offset");
    Demo.RequireEqual("record #4", seekResult.Message.Value, "record at offset 4");

    // ── Lookup by timestamp: first offset whose record time >= midTime ──
    var tpts = new[] { new TopicPartitionTimestamp(tp, new Timestamp(midTime, TimestampType.CreateTime)) };
    var offsets = consumer.OffsetsForTimes(tpts, TimeSpan.FromSeconds(10));
    Demo.Require(offsets.Count == 1, "OffsetsForTimes returned no result");
    var tsOffset = offsets[0].Offset;
    Demo.Require(tsOffset.Value >= 0, "OffsetsForTimes returned an invalid offset");
    Demo.Got($"[timestamp] midTime → offset {tsOffset.Value}");

    consumer.Seek(new TopicPartitionOffset(tp, tsOffset));
    var tsResult = ConsumeOne(consumer, TimeSpan.FromSeconds(10));
    Demo.Require(tsResult is not null, "timestamp seek read nothing");
    Demo.Got($"[timestamp] first record at/after midTime → '{tsResult!.Message.Value}'");
    // The record at that offset must be #midIndex (the first produced after midTime).
    Demo.RequireEqual($"record #{midIndex}", tsResult.Message.Value,
        "OffsetsForTimes points at the first record produced at/after midTime");

    consumer.Close();

    using (var admin = new AdminClientBuilder(KafkaClients.AdminConfig()).Build())
    {
        try { await admin.DeleteTopicsAsync([topic]); Demo.Step($"Cleaned up topic '{topic}'"); }
        catch (DeleteTopicsException) { /* best-effort */ }
    }

    Demo.Ok("Seek-by-offset and OffsetsForTimes (by-timestamp) both verified");
});

static ConsumeResult<string, string>? ConsumeOne(IConsumer<string, string> consumer, TimeSpan budget)
{
    var deadline = DateTime.UtcNow + budget;
    while (DateTime.UtcNow < deadline)
    {
        var cr = consumer.Consume(TimeSpan.FromSeconds(1));
        if (cr is not null) return cr;
    }
    return null;
}
