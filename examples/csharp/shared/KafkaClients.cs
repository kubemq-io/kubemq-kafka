using Confluent.Kafka;

namespace KubeMQ.Kafka.Examples.Shared;

/// <summary>
/// Shared connection helper for every KubeMQ Kafka C# example.
///
/// The KubeMQ Kafka connector is a Kafka wire-protocol listener (plain TCP 9092,
/// TLS 9093) that maps each Kafka topic "orders" onto the Events-Store channel
/// "kafka.orders". A native Confluent.Kafka app talks to it by ONLY pointing
/// BootstrapServers at the connector — no library swap.
///
/// The connector is DISABLED by default (gotcha #1): the server must be started
/// with CONNECTORS_KAFKA_ENABLE=true and, for external clients, a set
/// CONNECTORS_KAFKA_ADVERTISED_HOST (gotcha #2 — empty advertised host makes
/// clients connect then hang).
///
/// Confluent.Kafka is librdkafka-based, so its DEFAULT partitioner is CRC32
/// (consistent_random) — NOT the murmur2 used by Java/franz-go/kafkajs. Keyed
/// examples therefore land keys on the CRC32 partition; see gotcha #4.
/// </summary>
public static class KafkaClients
{
    /// <summary>Default connector bootstrap (plain-TCP 9092 listener).</summary>
    public const string DefaultBootstrap = "localhost:9092";

    /// <summary>KubeMQ Kafka channel prefix: topic "orders" ↔ channel "kafka.orders".</summary>
    public const string ChannelPrefix = "kafka.";

    /// <summary>
    /// Resolves the connector bootstrap servers from KUBEMQ_KAFKA_BOOTSTRAP,
    /// falling back to <see cref="DefaultBootstrap"/>. Used as the Kafka
    /// bootstrap.servers value (host:port list, not a URL — hence _BOOTSTRAP).
    /// </summary>
    public static string Bootstrap()
    {
        var v = Environment.GetEnvironmentVariable("KUBEMQ_KAFKA_BOOTSTRAP");
        return string.IsNullOrEmpty(v) ? DefaultBootstrap : v;
    }

    /// <summary>Base ProducerConfig pointed at the connector. Callers layer on
    /// Acks / EnableIdempotence / CompressionType / TransactionalId as needed.</summary>
    public static ProducerConfig ProducerConfig() => new()
    {
        BootstrapServers = Bootstrap(),
    };

    /// <summary>Base ConsumerConfig with a group id and start position.</summary>
    public static ConsumerConfig ConsumerConfig(
        string groupId,
        AutoOffsetReset offsetReset = AutoOffsetReset.Earliest,
        bool enableAutoCommit = false) => new()
        {
            BootstrapServers = Bootstrap(),
            GroupId = groupId,
            AutoOffsetReset = offsetReset,
            EnableAutoCommit = enableAutoCommit,
        };

    /// <summary>Base AdminClientConfig pointed at the connector.</summary>
    public static AdminClientConfig AdminConfig() => new()
    {
        BootstrapServers = Bootstrap(),
    };
}
