using MessageBroker;
using Newtonsoft.Json;

namespace MainApp;

public class Program
{
    private static MessageBus<string, string> _msgBus = null!;

    private const string TopicCmdName = "requests";
    private const string TopicGirls = "girls";
    private const string TopicBoys = "gachi";
    private const string TopicNumbers = "numbers";

    private static Producer<string, string> _producer = null!;

    private static async Task Prerequisites()
    {
        var topics = _msgBus.GetTopics();

        if (!topics.Contains(TopicCmdName))
        {
            await CreateTopic(_msgBus, TopicCmdName);
        }
        if (!topics.Contains(TopicGirls))
        {
            await CreateTopic(_msgBus, TopicGirls);
        }
        if (!topics.Contains(TopicBoys))
        {
            await CreateTopic(_msgBus, TopicBoys);
        }

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

    static async Task Main()
    {
        string word;

        _msgBus = new MessageBus<string, string>("names");
        await Prerequisites();

        Dictionary<string, (string description, Func<Task> command)> actions = new()
        {
            { "b", ("Get boy name", GetBoyName) },
            { "g", ("Get girl name", GetGirlName) },
            { "n", ("Get random numbers", GetNumbers) },
            { "q", ("Quit program", Quit) }
        };

        _producer = _msgBus.GetProducer();

        CancellationTokenSource cts = new();
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true; // prevent the process from terminating.
            cts.Cancel();
        };
        
        var boy = _msgBus.GetSubscriber(TopicBoys, (k, msg) =>
        {
            Console.WriteLine($"Received ♂boy♂ name: {msg}");
        }, cts.Token);

        var girl = _msgBus.GetSubscriber(TopicGirls, (k, msg) =>
        {
            Console.WriteLine($"Received girl name: {msg}");
        }, cts.Token);

        var numbers = _msgBus.GetSubscriber(TopicNumbers, (k, msg) =>
        {
            if (string.IsNullOrEmpty(msg))
                return;
            var lst = JsonConvert.DeserializeObject<List<int>>(msg);
            Console.WriteLine($"Got {lst.Count} numbers:");
            for (var i = 0; i < lst.Count; i++)
            {
                Console.WriteLine($"{i + 1}. {lst[i]}");
            }
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
        
        cts.Cancel();
        Console.WriteLine("Press any key to quit..");
        Console.ReadKey();
    }
    
    static async Task GetBoyName()
    {
        Console.WriteLine("# Requesting ♂college boy♂.. #");
        const string message = "get_boy_name";
        await _producer.SendMessageAsync(TopicCmdName, message, string.Empty);
    }
    static async Task GetNumbers()
    {
        Console.WriteLine("# Requesting random numbers.. #");
        const string message = "get_numbers";
        int number;
        do
        {
            Console.WriteLine("Type number of numbers (more than 0).");
        } while (!int.TryParse(Console.ReadLine(), out number) && number < 1);
        
        await _producer.SendMessageAsync(TopicCmdName, message, number.ToString());
    }

    static async Task GetGirlName()
    {
        Console.WriteLine("# Requesting random girl name.. #");
        const string message = "get_girl_name";
        await _producer.SendMessageAsync(TopicCmdName, message, string.Empty);
    }

    static async Task Quit()
    {
        Console.WriteLine("# Sending quit message.. #");
        const string message = "quit";
        await _producer.SendMessageAsync(TopicCmdName, message, string.Empty);
    }
}