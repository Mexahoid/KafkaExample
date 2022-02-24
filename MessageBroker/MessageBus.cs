using Confluent.Kafka;
using Confluent.Kafka.Admin;
namespace MessageBroker;

public class MessageBus<TK, TV>
{
    private readonly AdminClientBuilder _adminBuilder;
    private readonly string _host = Environment.GetEnvironmentVariable("DOTNET_RUNNING_IN_CONTAINER") == "true" ?
        "kafka:29092" :
        "127.0.0.1:9092";

    private readonly Action<string>? _logger;

    private MessageBus(Action<string>? logger = default)
    {
        _logger = logger;
        var config = new Dictionary<string, string>
        {
            {"bootstrap.servers", _host}
        };
        _adminBuilder = new AdminClientBuilder(config);
    }

    public static async Task<MessageBus<TK, TV>> Create(
        IEnumerable<string> topics,
        Action<string>? logger = default)
    {
        var mb = new MessageBus<TK, TV>(logger);
        await mb.EnsureTopicsCreated(topics);
        return mb;
    } 

    public Producer<TK, TV> GetProducer()
    {
        return new Producer<TK, TV>(_host, _logger);
    }

    public Subscriber<TK, TV> GetSubscriber(string topic, string group, Action<TK, TV> action, CancellationToken ct)
    {
        return new Subscriber<TK, TV>(topic, action, group, _host, ct);
    }

    private async Task EnsureTopicsCreated(IEnumerable<string> topics)
    {
        using var adminClient = _adminBuilder.Build();
        var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(10));
        var topicNames = metadata.Topics.Select(a => a.Topic).ToList();

        foreach (var topic in topics)
        {
            if (!topicNames.Contains(topic))
            {
                _logger?.Invoke($"Not found {topic}, trying to create");
                try
                {
                    await adminClient.CreateTopicsAsync(new[] {
                        new TopicSpecification { Name = topic, ReplicationFactor = 1, NumPartitions = 1 } });
                    _logger?.Invoke("Successfully created.");
                }
                catch (CreateTopicsException)
                {
                    _logger?.Invoke("Whoops, it seems that another process just created this topic.");
                }
            }
            else
            {
                _logger?.Invoke($"Topic {topic} found, all's OK.");
            }
        }
    }
}