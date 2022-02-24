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
    
    private static async Task Main()
    {
        Console.WriteLine("Initializing..");


        string[] topics = { TopicCmdName, TopicNumbers };
        _msgBus = await MessageBus<string, string>.Create(topics, Console.WriteLine);
        _prod = _msgBus.GetProducer();

        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true; // prevent the process from terminating.
            Cts.Cancel();
        };

        var sub = _msgBus.GetSubscriber(TopicCmdName, "numbers", ParseAction, Cts.Token);
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