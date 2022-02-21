using MessageBroker;
using Newtonsoft.Json;

namespace NumberService;

class Program
{
    private const string TopicCmdName = "requests";

    private const string TopicNumbers = "numbers";

    private static MessageBus<string, string> _msgBus = null!;
    private static readonly CancellationTokenSource Cts = new();

    private static readonly Random RNG = new();

    private static Producer<string, string> _prod = null!;

    private static async Task Prerequisites()
    {
        var topics = _msgBus.GetTopics();
        
        if (!topics.Contains(TopicNumbers))
        {
            await CreateTopic(_msgBus, TopicNumbers);
        }

        static async Task CreateTopic<TK, TV>(MessageBus<TK, TV> mb, string topic)
        {
            Console.WriteLine($"Not found {topic}, trying to create");
            try
            {
                await mb.CreateTopic(topic);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
        }
    }

    private static async Task Main()
    {
        Console.WriteLine("Initializing..");

        _msgBus = new MessageBus<string, string>("numbers");
        _prod = _msgBus.GetProducer();
        await Prerequisites();

        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true; // prevent the process from terminating.
            Cts.Cancel();
        };

        var sub = _msgBus.GetSubscriber(TopicCmdName, ParseAction, Cts.Token);
        Console.WriteLine("Awaiting");
        try
        {
            await Task.Delay(-1, Cts.Token);
        }
        catch (TaskCanceledException)
        {
            Console.WriteLine("Quitting");
        }
    }


    private static async void ParseAction(string key, string value)
    {
        switch (key)
        {
            case "get_numbers":
            {
                List<int> lst = new();
                int count = int.Parse(value);
                Console.WriteLine($"Received numbers command, sending sequence of {count} elements.");
                for (int i = 0; i < count; i++)
                {
                    lst.Add(RNG.Next(0, 500));
                }

                var msg = JsonConvert.SerializeObject(lst);
                await _prod.SendMessageAsync(TopicNumbers, key, msg);
                break;
            }
            case "quit":
            {
                Cts.Cancel();
                break;
            }
        }
    }
}