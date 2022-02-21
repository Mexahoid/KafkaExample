using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;

namespace MessageBroker;

public class MessageBus<TK, TV>
{
    private readonly ProducerBuilder<TK, TV> _producerBuilder;
    private readonly AdminClientBuilder _adminBuilder;
    private readonly ConsumerBuilder<TK, TV> _consBuilder;

    public MessageBus(string group)
    {
        const string host = "localhost";

        var producerConfig = new Dictionary<string, string>
        {
            {"bootstrap.servers", host}
        };
        _producerBuilder = new ProducerBuilder<TK, TV>(producerConfig);
        _adminBuilder = new AdminClientBuilder(producerConfig);
        

        var cc = new ConsumerConfig
        {
            BootstrapServers = host,
            GroupId = group,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true
        };

        _consBuilder = new ConsumerBuilder<TK, TV>(cc);
    }

    public void SendMessage(string topic, TK key, TV message)
    {
        using var producer = _producerBuilder.Build();
        producer.Produce(topic, new Message<TK, TV> { Key = key, Value = message },
            deliveryReport =>
            {
                Console.WriteLine(deliveryReport.Error.Code != ErrorCode.NoError
                    ? $"Failed to deliver message: {deliveryReport.Error.Reason}"
                    : $"Produced message to: {deliveryReport.TopicPartitionOffset}");
            });

        producer.Flush(TimeSpan.FromSeconds(10));
    }

    public Producer<TK, TV> GetProducer()
    {
        return new Producer<TK, TV>(this);
    }

    public Subscriber<TK, TV> GetSubscriber(string topic, Action<TK, TV> action, CancellationToken ct)
    {
        return new Subscriber<TK, TV>(topic, action, ct, this);
    }



    public async Task SendMessageAsync(string topic, TK key, TV message, CancellationToken ct = default)
    {
        using var producer = _producerBuilder.Build();
        var dr = await producer.ProduceAsync(topic, new Message<TK, TV> { Key = key, Value = message }, ct);

        Console.WriteLine($"Produced message to: {dr.TopicPartitionOffset}");

        producer.Flush(TimeSpan.FromSeconds(10));
    }

    public List<string> GetTopics()
    {
        using var adminClient = _adminBuilder.Build();
        var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(10));
        var topicNames = metadata.Topics.Select(a => a.Topic).ToList();
        return topicNames;
    }

    public async Task CreateTopic(string topic)
    {
        using var adminClient = _adminBuilder.Build();
        try
        {
            await adminClient.CreateTopicsAsync(new[] {
                new TopicSpecification { Name = topic, ReplicationFactor = 1, NumPartitions = 1 } });
        }
        catch (CreateTopicsException e)
        {
            throw new Exception("Whoops, it seems that another process just created this topic.", e);
        }
    }

    public void ConsumeContinuously(string topic, Action<TK, TV> action, CancellationToken ct)
    {
        using var consumer = _consBuilder.Build();
        consumer.Subscribe(topic);
        try
        {
            while (!ct.IsCancellationRequested)
            {
                var cr = consumer.Consume(ct);
                action(cr.Message.Key, cr.Message.Value);
            }
        }
        catch (OperationCanceledException)
        {
            //exception might have occurred since Ctrl-C was pressed.
        }
        finally
        {
            // Ensure the consumer leaves the group cleanly and final offsets are committed.
            consumer.Close();
        }
    }
}