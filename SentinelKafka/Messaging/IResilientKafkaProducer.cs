using System.Threading;
using System.Threading.Tasks;

namespace SentinelKafka.Messaging;

public interface IResilientKafkaProducer
{
    Task<bool> ProduceWithRetryAsync<T>(string topic, T message, string? key = null, CancellationToken ct = default);
}
