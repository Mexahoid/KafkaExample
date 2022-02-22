using Confluent.Kafka;

namespace MessageBroker;

public class Subscriber<TK, TV>
{
    private readonly Action<TK, TV> _messageReceived;
    
    public Subscriber(string topic, Action<TK, TV> action, string group, string host, CancellationToken ct)
    {
        _messageReceived += action;

        var cc = new ConsumerConfig
        {
            BootstrapServers = host,
            GroupId = group,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true
        };

        StartReceiving(cc, topic, ct);
    }

    private void StartReceiving(ConsumerConfig cc, string topic, CancellationToken token)
    {
        Task.Run(() =>
        {
            using var consumer = new ConsumerBuilder<TK, TV>(cc).Build();
            consumer.Subscribe(topic);
            try
            {
                while (!token.IsCancellationRequested)
                {
                    var cr = consumer.Consume(token);
                    _messageReceived(cr.Message.Key, cr.Message.Value);
                }
            }
            catch (OperationCanceledException)
            {
            }
            finally
            {
                consumer.Close();
            }
        }, token);
    }
    

}