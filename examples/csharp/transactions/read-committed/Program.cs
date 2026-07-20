using Confluent.Kafka;
using Confluent.Kafka.Admin;
using KubeMQ.Kafka.Examples.Shared;

// ─── Transactions 12: read_committed & the Last Stable Offset (LSO) ───
//
// Focuses on the CONSUMER side of EOS:
//   * read_committed consumers apply AbortedTransactions client-side (gotcha #12) —
//     the BROKER ships aborted records; the CLIENT drops them.
//   * While a transaction is OPEN, the read_committed high offset (the LSO) is
//     PINNED below the actual high-watermark (HWM): committed data past an open
//     txn is not yet readable.
//
// KIP-890 V1 ceiling (gotcha #9, spec §2.5) applies exactly as in eos-commit-abort:
// a same-epoch zombie can be admitted at V1 — upstream-shared, not a defect. This
// example asserts only what read_committed guarantees at V1.

return await Demo.RunAsync(async () =>
{
    const string topic = "kafka-ex-transactions-read-committed";
    var tp = new TopicPartition(topic, 0);
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
    txnConfig.EnableIdempotence = true;
    txnConfig.TransactionalId = $"cs-rc-{Guid.NewGuid():N}"; // no '/'
    using var producer = new ProducerBuilder<string, string>(txnConfig).Build();
    producer.InitTransactions(TimeSpan.FromSeconds(30));

    // 1) Commit a first transaction (2 records) so there IS committed data.
    producer.BeginTransaction();
    await producer.ProduceAsync(topic, new Message<string, string> { Value = "rc-committed-1" });
    await producer.ProduceAsync(topic, new Message<string, string> { Value = "rc-committed-2" });
    producer.CommitTransaction(TimeSpan.FromSeconds(30));
    Demo.Sent("committed txn #1 (2 records)");

    // 2) Open a SECOND transaction and produce, but DO NOT commit yet.
    producer.BeginTransaction();
    await producer.ProduceAsync(topic, new Message<string, string> { Value = "rc-open-3" });
    producer.Flush(TimeSpan.FromSeconds(10));
    Demo.Step("opened txn #2 and produced 'rc-open-3' (still uncommitted)");

    using (var rcConsumer = new ConsumerBuilder<string, string>(
        Cfg(IsolationLevel.ReadCommitted)).Build())
    using (var ruConsumer = new ConsumerBuilder<string, string>(
        Cfg(IsolationLevel.ReadUncommitted)).Build())
    {
        // While txn #2 is open, the read_committed high (LSO) is pinned STRICTLY
        // below the read_uncommitted high-watermark (HWM): the open txn holds the
        // LSO back, so its record is not yet stable/visible. We compare LSO to the
        // TRUE HWM, not a hard-coded offset — a committed txn also writes a commit
        // control-marker record that consumes an offset, so the raw offset is not
        // 1:1 with the data-record count (here LSO=3: rc-committed-1@0,
        // rc-committed-2@1, commit-marker@2; rc-open-3@3 is held back; HWM=4).
        var lso = rcConsumer.QueryWatermarkOffsets(tp, TimeSpan.FromSeconds(10)).High.Value;
        var hwm = ruConsumer.QueryWatermarkOffsets(tp, TimeSpan.FromSeconds(10)).High.Value;
        Demo.Got($"[read_committed] LSO (high)={lso} < HWM (read_uncommitted)={hwm} while txn open");
        Demo.Require(lso < hwm, "LSO must be pinned below the high-watermark while a txn is open");
    }

    // 3) Abort txn #2: 'rc-open-3' must never become visible to read_committed.
    producer.AbortTransaction(TimeSpan.FromSeconds(30));
    Demo.Step("aborted txn #2");

    // After the abort, the LSO catches up to the HWM (no open txn holds it back).
    using (var rcConsumer = new ConsumerBuilder<string, string>(
        Cfg(IsolationLevel.ReadCommitted)).Build())
    using (var ruConsumer = new ConsumerBuilder<string, string>(
        Cfg(IsolationLevel.ReadUncommitted)).Build())
    {
        var lsoAfter = rcConsumer.QueryWatermarkOffsets(tp, TimeSpan.FromSeconds(10)).High.Value;
        var hwmAfter = ruConsumer.QueryWatermarkOffsets(tp, TimeSpan.FromSeconds(10)).High.Value;
        Demo.Got($"[read_committed] after abort: LSO={lsoAfter} == HWM={hwmAfter} (LSO caught up)");
        Demo.RequireEqual(hwmAfter, lsoAfter, "LSO must catch up to the HWM after the open txn aborts");
    }

    // Read everything with read_committed: only the two committed records.
    using (var rcConsumer = new ConsumerBuilder<string, string>(
        Cfg(IsolationLevel.ReadCommitted, $"cs-rc-read-{Guid.NewGuid():N}")).Build())
    {
        rcConsumer.Subscribe(topic);
        var seen = new List<string>();
        var deadline = DateTime.UtcNow.AddSeconds(15);
        while (DateTime.UtcNow < deadline)
        {
            var cr = rcConsumer.Consume(TimeSpan.FromSeconds(2));
            if (cr is null) { if (seen.Count >= 2) break; continue; }
            seen.Add(cr.Message.Value);
            Demo.Got($"[read_committed] saw '{cr.Message.Value}'");
        }
        rcConsumer.Close();
        Demo.Require(seen.Contains("rc-committed-1") && seen.Contains("rc-committed-2"),
            "both committed records must be visible");
        Demo.Require(!seen.Contains("rc-open-3"), "aborted record must never be visible");
    }

    // Contrast: read_uncommitted DOES ship the aborted record (client-side filter
    // is what read_committed adds). We only assert read_committed's guarantee above;
    // this read is illustrative.
    using (var ruConsumer = new ConsumerBuilder<string, string>(
        Cfg(IsolationLevel.ReadUncommitted, $"cs-ru-read-{Guid.NewGuid():N}")).Build())
    {
        ruConsumer.Subscribe(topic);
        var seen = new List<string>();
        var deadline = DateTime.UtcNow.AddSeconds(8);
        while (DateTime.UtcNow < deadline)
        {
            var cr = ruConsumer.Consume(TimeSpan.FromSeconds(2));
            if (cr is null) continue;
            seen.Add(cr.Message.Value);
        }
        ruConsumer.Close();
        Demo.Got($"[read_uncommitted] saw {seen.Count} record(s) incl. aborted = {seen.Contains("rc-open-3")}");
    }

    await admin.DeleteTopicsAsync([topic]);
    Demo.Step($"Cleaned up topic '{topic}'");

    Demo.Ok("read_committed never sees aborted records; LSO pinned below HWM while a txn is open (KIP-890 V1)");
});

static ConsumerConfig Cfg(IsolationLevel level, string? group = null)
{
    var c = KafkaClients.ConsumerConfig(
        group ?? $"cs-rc-probe-{Guid.NewGuid():N}", AutoOffsetReset.Earliest);
    c.IsolationLevel = level;
    return c;
}
