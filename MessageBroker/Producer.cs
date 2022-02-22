using Confluent.Kafka;

namespace MessageBroker;

public class Producer<TK, TV>
{
    private readonly ProducerBuilder<TK, TV> _producerBuilder;
    private readonly Action<string>? _logger;

    public Producer(string host, Action<string>? logger = default)
    {
        _logger = logger;
        var producerConfig = new Dictionary<string, string>
        {
            {"bootstrap.servers", host}
        };
        _producerBuilder = new ProducerBuilder<TK, TV>(producerConfig);
    }

    public void SendMessage(string topic, TK key, TV message)
    {
        using var producer = _producerBuilder.Build();
        producer.Produce(topic, new Message<TK, TV> { Key = key, Value = message },
            deliveryReport =>
            {
                _logger?.Invoke(deliveryReport.Error.Code != ErrorCode.NoError
                    ? $"Failed to deliver message: {deliveryReport.Error.Reason}"
                    : $"Produced message to: {deliveryReport.TopicPartitionOffset}");
            });

        producer.Flush(TimeSpan.FromSeconds(10));
    }

    public async Task SendMessageAsync(string topic, TK key, TV message, CancellationToken ct = default)
    {
        using var producer = _producerBuilder.Build();
        try
        {
            var dr = await producer.ProduceAsync(topic, new Message<TK, TV> { Key = key, Value = message }, ct);
            _logger?.Invoke($"Produced message to: {dr.TopicPartitionOffset}");
        }
        catch (ProduceException<TK, TV> e)
        {
            _logger?.Invoke($"Failed to deliver message: {e.Error.Reason}");
        }

        producer.Flush(TimeSpan.FromSeconds(10));

    }
}