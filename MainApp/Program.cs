using MessageBroker;

namespace MainApp;

public class Program
{
    private static readonly MessageBus<string> MsgBus = MessageBus<string>.Instance;

    private const string TopicCmdName = "requests";
    private const string TopicGirls = "girls";
    private const string TopicBoys = "gachi";

    private static Producer<string> _producer = null!;

    private static async Task Prerequisites()
    {
        var topics = MsgBus.GetTopics();

        if (!topics.Contains(TopicCmdName))
        {
            await CreateTopic(TopicCmdName);
        }
        if (!topics.Contains(TopicGirls))
        {
            await CreateTopic(TopicGirls);
        }
        if (!topics.Contains(TopicBoys))
        {
            await CreateTopic(TopicBoys);
        }

        async Task CreateTopic(string topic)
        {
            Console.WriteLine($"Not found {topic}, trying to create");
            try
            {
                await MsgBus.CreateTopic(topic);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
        }
    }

    static async Task Main(string[] args)
    {
        string word;
        await Prerequisites();
        
        Dictionary<string, (string description, Func<Task> command)> actions = new()
        {
            { "b", ("Get boy name", GetBoyName) },
            { "g", ("Get girl name", GetGirlName) },
            { "q", ("Quit program", () => Task.CompletedTask) }
        };

        _producer = new Producer<string>();

        CancellationTokenSource cts = new();
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true; // prevent the process from terminating.
            cts.Cancel();
        };
        
        Subscriber<string> boy = new(TopicBoys, msg =>
        {
            Console.WriteLine($"Received ♂boy♂ name: {msg}");
        }, cts.Token);

        Subscriber<string> girl = new(TopicGirls, msg =>
        {
            Console.WriteLine($"Received girl name: {msg}");
        }, cts.Token);
        
        do
        {
            Console.WriteLine("# Available commands: #");

            foreach (var key in actions.Keys)
            {
                Console.WriteLine($"'{key}' - {actions[key].description}");
            }

            Console.Write(">> ");
            word = Console.ReadLine()!;
            if (string.IsNullOrEmpty(word))
                continue;
            if (!actions.ContainsKey(word))
                Console.WriteLine("! Unknown command, try again. !");
            else
                await actions[word].command();


        } while (word != "q");

        await Quit();

        cts.Cancel();
        Console.WriteLine("Press any key to quit..");
        Console.ReadKey();
    }
    
    static async Task GetBoyName()
    {
        Console.WriteLine("# Requesting ♂college boy♂.. #");
        const string message = "get_boy_name";
        await _producer.SendMessageAsync(TopicCmdName, message);
    }

    static async Task GetGirlName()
    {
        Console.WriteLine("# Requesting random girl name.. #");
        const string message = "get_girl_name";
        await _producer.SendMessageAsync(TopicCmdName, message);
    }
    static async Task Quit()
    {
        Console.WriteLine("# Sending quit message.. #");
        const string message = "quit";
        await _producer.SendMessageAsync(TopicCmdName, message);
    }
}