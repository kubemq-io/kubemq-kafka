using Confluent.Kafka;
using Confluent.Kafka.Admin;
using KubeMQ.Kafka.Examples.Shared;

// ─── Produce 1: Basic acks (produce acks 0/1/all + oversized → MESSAGE_TOO_LARGE) ───
//
// Produces to the Kafka topic "kafka-ex-produce-basic-acks", which the connector
// maps to Events-Store channel "kafka.kafka-ex-produce-basic-acks" (offset = STAN
// Sequence). Demonstrates:
//   * acks=All  — durable produce, DeliveryResult carries topic/partition/offset
//   * acks=1    — leader-ack produce (round-trip still asserted)
//   * gotcha #3 — on a multi-node cluster acks=0 on a follower silently drops;
//                 examples default to acks>=1
//   * oversized payload (> MaxMessageBytes, 1 MiB) → MESSAGE_TOO_LARGE
//
// Confluent.Kafka is librdkafka-based (idempotence is default-ON for acks=All; a
// non-all acks value needs it disabled — see gotcha in produce/idempotent).

return await Demo.RunAsync(async () =>
{
    const string topic = "kafka-ex-produce-basic-acks";

    // Ensure the topic exists (connector also auto-creates on Produce; we create
    // explicitly so partition count is deterministic).
    using (var admin = new AdminClientBuilder(KafkaClients.AdminConfig()).Build())
    {
        try
        {
            await admin.CreateTopicsAsync([
                new TopicSpecification { Name = topic, NumPartitions = 1, ReplicationFactor = 1 }
            ]);
            Demo.Step($"Created topic '{topic}' → channel '{KafkaClients.ChannelPrefix}{topic}'");
        }
        catch (CreateTopicsException e) when (
            e.Results.All(r => r.Error.Code == ErrorCode.TopicAlreadyExists))
        {
            Demo.Step($"Topic '{topic}' already exists");
        }
    }

    // ── acks=All: durable produce, assert the DeliveryResult round-trip ──
    var ackAllConfig = KafkaClients.ProducerConfig();
    ackAllConfig.Acks = Acks.All;
    using (var producer = new ProducerBuilder<string, string>(ackAllConfig).Build())
    {
        var dr = await producer.ProduceAsync(topic, new Message<string, string>
        {
            Key = "order",
            Value = "order #1001",
        });
        Demo.Sent($"acks=All produced to {dr.TopicPartitionOffset} (status={dr.Status})");
        Demo.Require(dr.Status == PersistenceStatus.Persisted, "acks=All message not Persisted");
        Demo.Require(dr.Offset.Value >= 0, "acks=All produced offset is negative");
    }

    // ── acks=1: leader-ack produce. Idempotence is default-ON and REQUIRES
    // acks=all, so we must disable it explicitly for a non-all acks value
    // (otherwise librdkafka raises a config conflict). ──
    var ack1Config = KafkaClients.ProducerConfig();
    ack1Config.Acks = Acks.Leader;
    ack1Config.EnableIdempotence = false;
    using (var producer = new ProducerBuilder<string, string>(ack1Config).Build())
    {
        var dr = await producer.ProduceAsync(topic, new Message<string, string>
        {
            Key = "order",
            Value = "order #1002",
        });
        Demo.Sent($"acks=1 produced to {dr.TopicPartitionOffset}");
        Demo.Require(dr.Offset.Value >= 0, "acks=1 produced offset is negative");
    }

    // ── Read the two records back to confirm the round-trip ──
    var consumerConfig = KafkaClients.ConsumerConfig(
        groupId: $"cs-basic-acks-{Guid.NewGuid():N}",
        offsetReset: AutoOffsetReset.Earliest);
    using (var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build())
    {
        consumer.Subscribe(topic);
        var seen = new List<string>();
        var deadline = DateTime.UtcNow.AddSeconds(15);
        while (seen.Count < 2 && DateTime.UtcNow < deadline)
        {
            var cr = consumer.Consume(TimeSpan.FromSeconds(2));
            if (cr is null) continue;
            seen.Add(cr.Message.Value);
            Demo.Got($"Consumed '{cr.Message.Value}' at {cr.TopicPartitionOffset}");
        }
        consumer.Close();
        Demo.Require(seen.Contains("order #1001"), "acks=All record not read back");
        Demo.Require(seen.Contains("order #1002"), "acks=1 record not read back");
    }

    // ── Oversized payload → MESSAGE_TOO_LARGE ──
    // Connector MaxMessageBytes is 1 MiB and its request frame cap is
    // MaxMessageBytes + 1 MiB slack = 2 MiB. A 1.5 MiB record is ABOVE the 1 MiB cap
    // but BELOW the 2 MiB frame cap, so it reaches the broker and is rejected with
    // MESSAGE_TOO_LARGE. A 2 MiB record would instead overflow the frame cap and
    // surface as a transport error, not MESSAGE_TOO_LARGE. We raise the client's own
    // MessageMaxBytes above the payload so the send leaves the client and the broker
    // (not the client) is the one that rejects it.
    var bigConfig = KafkaClients.ProducerConfig();
    bigConfig.Acks = Acks.All;
    bigConfig.MessageMaxBytes = 4 * 1024 * 1024; // above the 1.5 MiB payload so it reaches the broker
    using (var producer = new ProducerBuilder<string, string>(bigConfig).Build())
    {
        var oversized = new string('x', (1024 * 1024) + (512 * 1024)); // 1.5 MiB (over 1 MiB cap, under 2 MiB frame cap)
        try
        {
            await producer.ProduceAsync(topic, new Message<string, string>
            {
                Key = "big",
                Value = oversized,
            });
            throw new DemoFailure("oversized produce unexpectedly succeeded");
        }
        catch (ProduceException<string, string> e)
            when (e.Error.Code is ErrorCode.MsgSizeTooLarge or ErrorCode.InvalidMsgSize)
        {
            Demo.Step($"Oversized payload rejected: {e.Error.Code} (MESSAGE_TOO_LARGE)");
        }
    }

    // Cleanup: delete the topic so reruns start clean.
    using (var admin = new AdminClientBuilder(KafkaClients.AdminConfig()).Build())
    {
        try { await admin.DeleteTopicsAsync([topic]); Demo.Step($"Cleaned up topic '{topic}'"); }
        catch (DeleteTopicsException) { /* best-effort cleanup */ }
    }

    Demo.Ok("Basic-acks produce round-trip complete (acks=All, acks=1, oversized rejected)");
});
