using Confluent.Kafka;
using Confluent.Kafka.Admin;
using KubeMQ.Kafka.Examples.Shared;

// ─── Produce 2: Idempotent producer (exactly-once produce dedup) ───
//
// EnableIdempotence=true makes librdkafka call InitProducerId (ApiKey 22) to
// obtain a Producer Id (PID). Each record carries (PID, epoch, sequence); the
// broker dedups per (PID, partition), so a producer-side retry never appends a
// duplicate. Idempotence forces acks=all under the hood.
//
// We produce N distinct records with idempotence ON and a retry budget
// (MessageSendMaxRetries), then consume everything back and assert EXACTLY N
// records — librdkafka's internal produce retries (the retriable window) must not
// append duplicates. NOTE: this is the happy-path proof; it does not fault-inject a
// broker-side retry, so it verifies the client+connector do not self-duplicate
// rather than exercising every dedup path.

return await Demo.RunAsync(async () =>
{
    const string topic = "kafka-ex-produce-idempotent";
    const int count = 5;

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

    // Idempotent producer: EnableIdempotence auto-sets Acks=All and enables the
    // InitProducerId handshake. A short RetryBackoff + high retries keeps the
    // per-(PID,partition) sequence guard active on transient errors.
    var config = KafkaClients.ProducerConfig();
    config.EnableIdempotence = true;
    config.Acks = Acks.All;
    config.MessageSendMaxRetries = 10;
    config.RetryBackoffMs = 50;

    using (var producer = new ProducerBuilder<string, string>(config).Build())
    {
        for (var i = 1; i <= count; i++)
        {
            var dr = await producer.ProduceAsync(topic, new Message<string, string>
            {
                Key = "order",
                Value = $"idempotent #{i}",
            });
            Demo.Require(dr.Status == PersistenceStatus.Persisted, $"record #{i} not Persisted");
            Demo.Sent($"produced 'idempotent #{i}' at {dr.TopicPartitionOffset}");
        }
        // Flush to force delivery of any in-flight retries before we read back.
        producer.Flush(TimeSpan.FromSeconds(10));
    }

    // Consume everything back: with idempotence ON, exactly `count` records exist,
    // each unique — no duplicates from any internal retry.
    var consumerConfig = KafkaClients.ConsumerConfig(
        groupId: $"cs-idempotent-{Guid.NewGuid():N}",
        offsetReset: AutoOffsetReset.Earliest);
    using (var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build())
    {
        consumer.Subscribe(topic);
        var seen = new List<string>();
        var deadline = DateTime.UtcNow.AddSeconds(15);
        while (DateTime.UtcNow < deadline)
        {
            var cr = consumer.Consume(TimeSpan.FromSeconds(2));
            if (cr is null)
            {
                if (seen.Count >= count) break;
                continue;
            }
            seen.Add(cr.Message.Value);
            Demo.Got($"consumed '{cr.Message.Value}' at {cr.TopicPartitionOffset}");
        }
        consumer.Close();

        Demo.RequireEqual(count, seen.Count, "record count (idempotence must yield no duplicates)");
        Demo.RequireEqual(count, seen.Distinct().Count(), "distinct record count");
    }

    using (var admin = new AdminClientBuilder(KafkaClients.AdminConfig()).Build())
    {
        try { await admin.DeleteTopicsAsync([topic]); Demo.Step($"Cleaned up topic '{topic}'"); }
        catch (DeleteTopicsException) { /* best-effort */ }
    }

    Demo.Ok($"Idempotent produce complete: {count} records, no duplicates (PID dedup via InitProducerId)");
});
