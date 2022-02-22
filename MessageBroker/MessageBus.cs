using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Confluent.Kafka.SyncOverAsync;
namespace MessageBroker;

public class MessageBus<TK, TV>
{
    private readonly AdminClientBuilder _adminBuilder;
    private readonly string _host = Environment.GetEnvironmentVariable("DOTNET_RUNNING_IN_CONTAINER") == "true" ?
        "kafka:29092" :
        "127.0.0.1:9092";

    private readonly Action<string>? _logger;

    public MessageBus(Action<string>? logger = default)
    {
        _logger = logger;
        var config = new Dictionary<string, string>
        {
            {"bootstrap.servers", _host}
        };
        _adminBuilder = new AdminClientBuilder(config);
    }
    
    public Producer<TK, TV> GetProducer()
    {
        return new Producer<TK, TV>(_host, _logger);
    }

    public Subscriber<TK, TV> GetSubscriber(string topic, string group, Action<TK, TV> action, CancellationToken ct)
    {
        return new Subscriber<TK, TV>(topic, action, group, _host, ct);
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
        catch (CreateTopicsException)
        {
            _logger?.Invoke("Whoops, it seems that another process just created this topic.");
        }
    }
}