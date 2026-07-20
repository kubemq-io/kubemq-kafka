using Confluent.Kafka;
using Confluent.Kafka.Admin;
using KubeMQ.Kafka.Examples.Shared;

// ─── Admin 8: topic lifecycle (create / describe / cluster / delete) ───
//
// Full AdminClient lifecycle:
//   * CreateTopics       — create a topic with an explicit config
//   * DescribeConfigs    — read the topic's effective config back
//   * DescribeCluster    — via GetMetadata: broker list + controller
//   * DeleteTopics       — remove it
//   * gotcha #6          — a topic name containing '~' is rejected
//                          (INVALID_TOPIC_EXCEPTION, ErrorCode.TopicException = 17)

return await Demo.RunAsync(async () =>
{
    const string topic = "kafka-ex-admin-topics-lifecycle";
    using var admin = new AdminClientBuilder(KafkaClients.AdminConfig()).Build();

    // ── CreateTopics with an explicit config ──
    try
    {
        await admin.CreateTopicsAsync([
            new TopicSpecification
            {
                Name = topic,
                NumPartitions = 2,
                ReplicationFactor = 1,
                Configs = new Dictionary<string, string> { ["retention.ms"] = "3600000" },
            }
        ]);
        Demo.Step($"Created topic '{topic}' (2 partitions, retention.ms=3600000)");
    }
    catch (CreateTopicsException e) when (
        e.Results.All(r => r.Error.Code == ErrorCode.TopicAlreadyExists))
    {
        Demo.Step($"Topic '{topic}' already exists");
    }

    // ── DescribeConfigs: read the topic config back ──
    var resource = new ConfigResource { Type = ResourceType.Topic, Name = topic };
    var described = await admin.DescribeConfigsAsync([resource]);
    var entries = described[0].Entries;
    Demo.Require(entries.ContainsKey("retention.ms"), "retention.ms missing from DescribeConfigs");
    Demo.Got($"[describe] retention.ms = {entries["retention.ms"].Value}");

    // ── DescribeCluster via GetMetadata: brokers + controller ──
    var metadata = admin.GetMetadata(TimeSpan.FromSeconds(10));
    Demo.Require(metadata.Brokers.Count >= 1, "cluster metadata reports no brokers");
    Demo.Got($"[cluster] {metadata.Brokers.Count} broker(s); topic present = " +
             $"{metadata.Topics.Any(t => t.Topic == topic)}");

    // ── gotcha #6: a '~' in a topic name is rejected ──
    const string badName = "bad~topic";
    try
    {
        await admin.CreateTopicsAsync([
            new TopicSpecification { Name = badName, NumPartitions = 1, ReplicationFactor = 1 }
        ]);
        throw new DemoFailure($"topic name '{badName}' with '~' was unexpectedly accepted");
    }
    catch (CreateTopicsException e)
    {
        var code = e.Results[0].Error.Code;
        Demo.Step($"[gotcha #6] '{badName}' rejected: {code}");
        Demo.Require(
            code is ErrorCode.TopicException or ErrorCode.InvalidRequest or ErrorCode.Local_UnknownTopic,
            $"expected INVALID_TOPIC_EXCEPTION-style rejection for '~', got {code}");
    }

    // ── DeleteTopics ──
    await admin.DeleteTopicsAsync([topic]);
    Demo.Step($"Deleted topic '{topic}'");

    Demo.Ok("Topic lifecycle verified: create → describe → cluster → delete (+ '~' rejected, gotcha #6)");
});
