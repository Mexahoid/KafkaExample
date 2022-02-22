using MessageBroker;

namespace NameService;

class Program
{
    private static readonly string[] BoyNames =
    {
        "Arsenii",
        "Igor",
        "Kostya",
        "Ivan",
        "Dmitrii",
    };
    private static readonly string[] GirlNames =
    {
        "Nastya",
        "Lena",
        "Ksusha",
        "Katya",
        "Olga"
    };


    private const string TopicCmdName = "requests";
    private const string TopicGirls = "girls";
    private const string TopicBoys = "gachi";


    private static readonly CancellationTokenSource Cts = new();
    private static MessageBus<string, string> _msgBusNames = null!;
    private static Producer<string, string> _prod = null!;
    
    static async Task Main()
    {
        Console.WriteLine("Initializing..");

        _msgBusNames = new MessageBus<string, string>(Console.WriteLine);
        _prod = _msgBusNames.GetProducer();
        await _msgBusNames.EnsureTopicsCreated(TopicCmdName, TopicBoys, TopicGirls);
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true; // prevent the process from terminating.
            Cts.Cancel();
        };

        var sub = _msgBusNames.GetSubscriber(TopicCmdName, "main", ParseAction, Cts.Token);
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
            case "get_boy_name":
            {
                string name = GetRandomCollegeBoy();
                Console.WriteLine($"Received boy command, sending name '{name}'");
                await _prod.SendMessageAsync(TopicBoys, key, name);
                break;
            }
            case "get_girl_name":
            {
                string name = GetRandomGirl();
                Console.WriteLine($"Received girl command, sending name '{name}'");
                await _prod.SendMessageAsync(TopicGirls, key, name);
                break;
            }
            case "quit":
            {
                Cts.Cancel();
                break;
            }
        }
    }

    private static string GetRandomCollegeBoy()
    {
        var r = new Random().Next(0, 5);
        var randName = BoyNames[r];
        return randName;
    }

    private static string GetRandomGirl()
    {
        var r = new Random().Next(0, 5);
        var randName = GirlNames[r];
        return randName;
    }
}