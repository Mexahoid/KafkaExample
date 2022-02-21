namespace MessageBroker;

public class Producer<TK, TV>
{
    private readonly MessageBus<TK, TV> _host;
    public Producer(MessageBus<TK, TV> mb)
    {
        _host = mb;
    }

    public void SendMessage(string topic, TK key, TV message)
    {
        _host.SendMessage(topic, key, message);
    }

    public async Task SendMessageAsync(string topic, TK key, TV message)
    {
        await _host.SendMessageAsync(topic, key, message);
    }
}