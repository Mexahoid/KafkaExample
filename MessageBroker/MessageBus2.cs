using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace MessageBroker;

public class MessageBus2<TV>
{
    private static MessageBus2<TV>? _instance;
    public static MessageBus2<TV> Instance => _instance ??= new MessageBus2<TV>();


    private readonly ProducerBuilder<Null, TV> _producerBuilder;
    private readonly AdminClientBuilder _adminBuilder;
    private readonly ConsumerBuilder<Null, TV> _consBuilder;

    private readonly List<(Guid, IConsumer<Null, TV>, Task)> _consumers;


    private MessageBus2()
    {
        const string host = "localhost";

        var producerConfig = new Dictionary<string, string>
        {
            {"bootstrap.servers", host}
        };
        _producerBuilder = new ProducerBuilder<Null, TV>(producerConfig);
        _adminBuilder = new AdminClientBuilder(producerConfig);


        var consumerConfig = new Dictionary<string, string>
        {
            {"group.id", "custom-group"},
            {"bootstrap.servers", host}
        };
        _consBuilder = new ConsumerBuilder<Null, TV>(consumerConfig);
        _consumers = new List<(Guid, IConsumer<Null, TV>, Task)>();
    }

    public void SendMessage(string topic, TV message)
    {
        using var producer = _producerBuilder.Build();
        producer.Produce(topic, new Message<Null, TV> { Value = message },
            (deliveryReport) =>
            {
                Console.WriteLine(deliveryReport.Error.Code != ErrorCode.NoError
                    ? $"Failed to deliver message: {deliveryReport.Error.Reason}"
                    : $"Produced message to: {deliveryReport.TopicPartitionOffset}");
            });

        producer.Flush(TimeSpan.FromSeconds(10));
    }
    public async Task SendMessageAsync(string topic, TV message, CancellationToken ct = default)
    {
        using var producer = _producerBuilder.Build();
        var dr = await producer.ProduceAsync(topic, new Message<Null, TV> { Value = message }, ct);

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
            throw new Exception($"An error occured when creating topic {e.Results[0].Topic}: {e.Results[0].Error.Reason}", e);
        }
    }

    public bool CheckTopic(string topic)
    {
        var topics = GetTopics();
        return topics.Contains(topic);
    }

    public async Task ConsumeContinuously(string topic, Action<TV> action)
    {
        CancellationTokenSource cts = new();
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true; // prevent the process from terminating.
            cts.Cancel();
        };


        await Task.Run(() =>
        {
            using var consumer = _consBuilder.Build();
            consumer.Subscribe(topic);
            try
            {
                while (!cts.IsCancellationRequested)
                {
                    var cr = consumer.Consume(cts.Token);
                    action(cr.Message.Value);
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
        }, cts.Token);
    }

    public async Task ConsumeOnce(string topic, Action<TV> action)
    {
        CancellationTokenSource cts = new();
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true; // prevent the process from terminating.
            cts.Cancel();
        };


        await Task.Run(() =>
        {
            using var consumer = _consBuilder.Build();
            consumer.Subscribe(topic);
            try
            {
                var cr = consumer.Consume(cts.Token);
                action(cr.Message.Value);
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
        }, cts.Token);
    }
}