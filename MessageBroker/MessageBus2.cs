using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace MessageBroker;

public class MessageBus2<TK, TV>
{
    private static MessageBus2<TK, TV>? _instance;
    public static MessageBus2<TK, TV> Instance => _instance ??= new MessageBus2<TK, TV>();


    private readonly ProducerBuilder<TK, TV> _producerBuilder;
    private readonly AdminClientBuilder _adminBuilder;
    private readonly ConsumerBuilder<TK, TV> _consBuilder;

    private readonly List<(Guid, IConsumer<TK, TV>, Task)> _consumers;


    private MessageBus2()
    {
        const string host = "localhost";

        var producerConfig = new Dictionary<string, string>
        {
            {"bootstrap.servers", host}
        };
        _producerBuilder = new ProducerBuilder<TK, TV>(producerConfig);
        _adminBuilder = new AdminClientBuilder(producerConfig);


        var consumerConfig = new Dictionary<string, string>
        {
            {"group.id", "custom-group"},
            {"bootstrap.servers", host}
        };
        _consBuilder = new ConsumerBuilder<TK, TV>(consumerConfig);
        _consumers = new List<(Guid, IConsumer<TK, TV>, Task)>();
    }
    
    public void SendMessage(string topic, TK key, TV message)
    {
        using var producer = _producerBuilder.Build();
        producer.Produce(topic, new Message<TK, TV> { Key = key, Value = message },
            (deliveryReport) =>
            {
                Console.WriteLine(deliveryReport.Error.Code != ErrorCode.NoError
                    ? $"Failed to deliver message: {deliveryReport.Error.Reason}"
                    : $"Produced message to: {deliveryReport.TopicPartitionOffset}");
            });

        producer.Flush(TimeSpan.FromSeconds(10));

        Console.WriteLine($"Message '{message}' was sent to topic '{topic}'");
    }

    public List<string> GetTopics()
    {
        using var adminClient = _adminBuilder.Build();
        var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(10));
        var topicNames = metadata.Topics.Select(a => a.Topic).ToList();
        return topicNames;
    }

    public async Task DeleteTopic(string topic)
    {
        using var adminClient = _adminBuilder.Build();
        try
        {
            await adminClient.DeleteTopicsAsync(new []
            {
                topic
            });
        }
        catch (DeleteTopicsException e)
        {
            throw new Exception($"An error occured when deleting topic {e.Results[0].Topic}: {e.Results[0].Error.Reason}", e);
        }
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



    public void Consume(string topic)
    {
        CancellationTokenSource cts = new();
        Console.CancelKeyPress += (_, e) => {
            e.Cancel = true; // prevent the process from terminating.
            cts.Cancel();
        };

        using var consumer = _consBuilder.Build();
        consumer.Subscribe(topic);
        try
        {
            while (true)
            {
                var cr = consumer.Consume(cts.Token);
                var key = cr.Message.Key == null ? default : cr.Message.Key;
                Console.WriteLine($"Consumed record with key '{key}' and value '{cr.Message.Value}'");
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
    
    /*
    public Guid Subscribe(string topic, Action<string> action, CancellationTokenSource cts)
    {
        var consumer = _consBuilder.Build();
        try
        {
            consumer.Subscribe(topic);
            Console.WriteLine($"Subscribed on topic '{topic}'");
        }
        catch (Exception e)
        {
            Console.WriteLine(e.Message);
        }

        var task = new Task(() =>
        {
            while (!cts.Token.IsCancellationRequested)
            {
                var answer = consumer.Consume(TimeSpan.FromMilliseconds(10));
                if (answer != null)
                {
                    action(answer.Message.Value);
                }
            }
        });

        Guid res = Guid.NewGuid();
        _consumers.Add((res, consumer, task));
        task.Start();

        return res;
    }

    public void Unsubscribe(Guid guid, CancellationTokenSource cts)
    {
        cts.Cancel();
        _consumers.RemoveAll(x => x.Item1 == guid);
    }


    public string AwaitMessageAsync(string topic, CancellationToken ct)
    {

        var consumer = _consBuilder.Build();

        try
        {
            consumer.Subscribe(topic);
        }
        catch (Exception e)
        {
            Console.WriteLine(e.Message);
        }

        consumer.Assign(new List<TopicPartitionOffset>
        {
            new(topic, 0, -1)
        });

        while (!ct.IsCancellationRequested)
        {
            var answer = consumer.Consume(TimeSpan.FromMilliseconds(10));
            if (answer != null)
            {
                //consumer.Dispose();
                return answer.Message.Value;
            }
        }

        return string.Empty;
    }*/
    /*public async Task<string> SubscribeOnTopic(string topic, Action<string> action, CancellationToken ct)
    {
        var _consBuilder = new ConsumerBuilder<Null, string>(consumerConfig);
        using var consumer = _consBuilder.Build();

        try
        {
            consumer.Subscribe(topic);
        }
        catch (Exception e)
        {
            Console.WriteLine(e.Message);
        }
        

        while (!ct.IsCancellationRequested)
        {
            var answer = consumer.Consume(TimeSpan.FromMilliseconds(10));
            if (answer != null)
            {
                action(answer.Message.Value);
            }
            await Task.Delay(1, ct);
        }

        return string.Empty;
    }*/
}