namespace MessageBroker;

public class Producer<TV>
{
    public void SendMessage(string topic, TV message)
    {
        MessageBus<TV>.Instance.SendMessage(topic, message);
    }

    public async Task SendMessageAsync(string topic, TV message)
    {
        await MessageBus<TV>.Instance.SendMessageAsync(topic, message);
    }
}