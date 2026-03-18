using System.Threading.Tasks;

namespace SentinelKafka.Messaging;

public interface IKafkaProducer
{
    Task ProduceAsync(string topic, string value);
    Task ProduceAsync<T>(string topic, T payload, string? key = null);
}
