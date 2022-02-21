namespace MessageBroker;

public class Subscriber<TK, TV>
{
    private event Action<TK, TV> MessageReceived;

    private readonly string _topic;

    private readonly CancellationToken _token;

    private readonly MessageBus<TK, TV> _host;

    public Subscriber(string topic, Action<TK, TV> action, CancellationToken ct, MessageBus<TK, TV> mb)
    {
        _topic = topic;
        MessageReceived += action;
        _token = ct;
        _host = mb;
        StartReceiving();
    }

    private void StartReceiving()
    {
        Task.Run(() =>
        {
            _host.ConsumeContinuously(_topic, MessageReceived, _token);
        }, _token);
    }

}