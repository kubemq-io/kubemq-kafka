using Confluent.Kafka;
using Confluent.Kafka.Admin;
using KubeMQ.Kafka.Examples.Shared;

// ─── Consumer-groups 6: join / rebalance across two consumers ───
//
// Two consumers in the SAME group subscribe to a multi-partition topic. The group
// coordinator runs Join/Sync/Heartbeat so the partitions redistribute across both
// members. This example asserts NO LOSS: every produced record is observed at least
// once across the two members even though ownership moves during the rebalance.
//
// It does NOT assert exactly-once. With EnableAutoCommit=false and
// AutoOffsetReset.Earliest, a partition reassigned mid-run has no committed offset
// on its new owner, so it is re-read from the log start — a record CAN be observed
// more than once. The distinct-value collector below verifies coverage (no loss);
// exactly-once delivery is out of scope here (see transactions/ for EOS).

return await Demo.RunAsync(async () =>
{
    const string topic = "kafka-ex-consumer-groups-join-rebalance";
    const int partitions = 4;
    const int total = 40;
    var groupId = $"cs-join-rebalance-{Guid.NewGuid():N}";

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

    // Produce the full set spread across partitions (keyed by index).
    var producerConfig = KafkaClients.ProducerConfig();
    producerConfig.Acks = Acks.All;
    using (var producer = new ProducerBuilder<string, string>(producerConfig).Build())
    {
        for (var i = 0; i < total; i++)
            await producer.ProduceAsync(topic, new Message<string, string>
            {
                Key = $"k{i}",
                Value = $"msg #{i}",
            });
        producer.Flush(TimeSpan.FromSeconds(15));
        Demo.Sent($"produced {total} records across {partitions} partitions");
    }

    // Shared collectors across the two members.
    var seen = new System.Collections.Concurrent.ConcurrentDictionary<string, int>();
    using var stopAll = new CancellationTokenSource(TimeSpan.FromSeconds(40));

    IConsumer<string, string> BuildMember(string name)
    {
        return new ConsumerBuilder<string, string>(
                KafkaClients.ConsumerConfig(groupId, AutoOffsetReset.Earliest))
            .SetPartitionsAssignedHandler((_, parts) =>
                Console.WriteLine($"[*] {name} assigned: [{string.Join(", ", parts.Select(p => p.Partition.Value))}]"))
            .SetPartitionsRevokedHandler((_, parts) =>
                Console.WriteLine($"[*] {name} revoked: [{string.Join(", ", parts.Select(p => p.Partition.Value))}]"))
            .Build();
    }

    async Task RunMember(string name, TimeSpan startDelay)
    {
        await Task.Delay(startDelay);
        using var consumer = BuildMember(name);
        consumer.Subscribe(topic);
        try
        {
            while (!stopAll.IsCancellationRequested)
            {
                var cr = consumer.Consume(TimeSpan.FromSeconds(1));
                if (cr is null)
                {
                    if (seen.Count >= total) break;
                    continue;
                }
                seen.AddOrUpdate(cr.Message.Value, 1, (_, c) => c + 1);
                Console.WriteLine($"[v] {name} got '{cr.Message.Value}' from partition {cr.Partition.Value}");
                if (seen.Count >= total) break;
            }
        }
        finally { consumer.Close(); }
    }

    // Member 1 starts first (owns all partitions), member 2 joins shortly after to
    // force a rebalance mid-consumption.
    var m1 = RunMember("member-1", TimeSpan.Zero);
    var m2 = RunMember("member-2", TimeSpan.FromSeconds(2));
    await Task.WhenAll(m1, m2);

    // No loss: every produced record must be seen; exactly-once across the group.
    Demo.RequireEqual(total, seen.Count, "distinct records consumed across the group");
    for (var i = 0; i < total; i++)
        Demo.Require(seen.ContainsKey($"msg #{i}"), $"record 'msg #{i}' was lost across rebalance");

    using (var admin = new AdminClientBuilder(KafkaClients.AdminConfig()).Build())
    {
        try { await admin.DeleteTopicsAsync([topic]); Demo.Step($"Cleaned up topic '{topic}'"); }
        catch (DeleteTopicsException) { /* best-effort */ }
    }

    Demo.Ok($"Rebalance across 2 members: all {total} records consumed, no loss");
});
