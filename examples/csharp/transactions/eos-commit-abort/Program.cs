using Confluent.Kafka;
using Confluent.Kafka.Admin;
using KubeMQ.Kafka.Examples.Shared;

// ─── Transactions 11: EOS commit vs abort ───
//
// Transactional producer flow:
//   InitTransactions → BeginTransaction → ProduceAsync… → CommitTransaction
//                                                       \→ AbortTransaction
// A read_committed consumer sees COMMITTED records and NEVER the aborted ones.
//
// ── KIP-890 ceiling (gotcha #9, spec §2.5) ──────────────────────────────────────
// The connector implements the transaction-coordinator surface at the KIP-890 V1
// level. A same-epoch "zombie" producer can still be admitted in narrow races —
// this is an UPSTREAM-SHARED protocol ceiling, NOT a connector defect and NOT a
// failure of this example. Do not claim guarantees beyond spec §2 (V1). See
// docs/concepts/transactions-eos.md.
//
// gotcha #7: a transactional.id MUST NOT contain '/' (→ INVALID_REQUEST 42) — we
// use a 'cs-eos-<uuid>' shape with no '/'.

return await Demo.RunAsync(async () =>
{
    const string topic = "kafka-ex-transactions-eos-commit-abort";
    using var admin = new AdminClientBuilder(KafkaClients.AdminConfig()).Build();
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

    var txnConfig = KafkaClients.ProducerConfig();
    txnConfig.EnableIdempotence = true;                     // required for transactions
    txnConfig.TransactionalId = $"cs-eos-{Guid.NewGuid():N}"; // no '/' — gotcha #7
    using (var producer = new ProducerBuilder<string, string>(txnConfig).Build())
    {
        producer.InitTransactions(TimeSpan.FromSeconds(30)); // InitProducerId with txn coordinator
        Demo.Step($"InitTransactions ok (transactional.id='{txnConfig.TransactionalId}')");

        // ── committed transaction ──
        producer.BeginTransaction();
        await producer.ProduceAsync(topic, new Message<string, string> { Value = "committed-A" });
        await producer.ProduceAsync(topic, new Message<string, string> { Value = "committed-B" });
        producer.CommitTransaction(TimeSpan.FromSeconds(30));
        Demo.Sent("committed txn with 2 records (committed-A, committed-B)");

        // ── aborted transaction ──
        producer.BeginTransaction();
        await producer.ProduceAsync(topic, new Message<string, string> { Value = "aborted-X" });
        await producer.ProduceAsync(topic, new Message<string, string> { Value = "aborted-Y" });
        producer.AbortTransaction(TimeSpan.FromSeconds(30));
        Demo.Sent("aborted txn with 2 records (aborted-X, aborted-Y)");
    }

    // ── read_committed consumer: sees committed, never aborted ──
    var consumerConfig = KafkaClients.ConsumerConfig(
        groupId: $"cs-eos-read-{Guid.NewGuid():N}", offsetReset: AutoOffsetReset.Earliest);
    consumerConfig.IsolationLevel = IsolationLevel.ReadCommitted;
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
                if (seen.Count >= 2) break;
                continue;
            }
            seen.Add(cr.Message.Value);
            Demo.Got($"[read_committed] saw '{cr.Message.Value}'");
        }
        consumer.Close();

        Demo.Require(seen.Contains("committed-A"), "committed-A must be visible");
        Demo.Require(seen.Contains("committed-B"), "committed-B must be visible");
        Demo.Require(!seen.Contains("aborted-X"), "aborted-X must NOT be visible");
        Demo.Require(!seen.Contains("aborted-Y"), "aborted-Y must NOT be visible");
    }

    await admin.DeleteTopicsAsync([topic]);
    Demo.Step($"Cleaned up topic '{topic}'");

    Demo.Ok("EOS verified: committed records visible, aborted records absent under read_committed (KIP-890 V1)");
});
