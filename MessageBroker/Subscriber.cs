namespace MessageBroker;

public class Subscriber<TV>
{
    private event Action<TV> MessageReceived;

    private readonly string _topic;

    private readonly CancellationToken _token;

    public Subscriber(string topic, Action<TV> action, CancellationToken ct)
    {
        _topic = topic;
        MessageReceived += action;
        _token = ct;

        StartReceiving();
    }

    private void StartReceiving()
    {
        Task.Run(async () =>
        {
            await MessageBus<TV>.Instance.ConsumeContinuously(_topic, MessageReceived, _token);
        }, _token);
    }

}