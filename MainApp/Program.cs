using System;
using MessageBroker;

namespace MainApp;

public class Program
{
    private static readonly MessageBus2<string> MsgBus = MessageBus2<string>.Instance;

    private const string TopicCmdName = "requests";
    private const string TopicGirls = "girls";
    private const string TopicBoys = "gachi";

    private static Queue<(string topic, string message)> _messages = null!;

    static async Task CreateTopic(string topic)
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

    static async Task Main(string[] args)
    {
        string word;
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

        _messages = new Queue<(string, string)>();
        Task.Run(Consume).GetAwaiter().GetResult();
        Task.Run(ProcessMessage).GetAwaiter().GetResult();

        Dictionary<string, (string description, Func<Task> command)> actions = new()
        {
            { "l", ("List all available topics", ListTopic) },
            { "s", ("Send message to topic", SendMessageToTopic) },
            { "b", ("Get boy name", GetBoyName) },
            { "g", ("Get girl name", GetGirlName) },
            { "q", ("Quit program", () => Task.CompletedTask) }
        };

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

    }

    static Task ListTopic()
    {
        var topics = MsgBus.GetTopics();

        Console.WriteLine("# Available topics: #");
        for (var i = 0; i < topics.Count; i++)
        {
            Console.WriteLine($"{i + 1}. {topics[i]}");
        }
        return Task.CompletedTask;
    }


    static Task SendMessageToTopic()
    {
        Console.WriteLine("# Send message #");

        var topic = string.Empty;

        while (string.IsNullOrEmpty(topic))
        {
            Console.Write("Type in topic name >> ");
            topic = Console.ReadLine();
        }

        var message = string.Empty;

        while (string.IsNullOrEmpty(message))
        {
            Console.Write("Type in message >> ");
            message = Console.ReadLine();
        }

        MsgBus.SendMessage(topic, message);
        return Task.CompletedTask;
    }

    static async Task GetBoyName()
    {
        Console.WriteLine("# Get random ♂boy♂ name #");

        const string message = "get_boy_name";

        await MsgBus.SendMessageAsync(TopicCmdName, message);
    }

    static async Task GetGirlName()
    {
        Console.WriteLine("# Get random girl name #");

        const string message = "get_girl_name";

        await MsgBus.SendMessageAsync(TopicCmdName, message);
    }


    private static void Consume()
    {
        Task.Run(async () => await MsgBus.ConsumeContinuously(TopicGirls, msg =>
        {
            _messages.Enqueue((TopicGirls, msg));
        }));
        Task.Run(async () => await MsgBus.ConsumeContinuously(TopicBoys, msg =>
        {
            _messages.Enqueue((TopicBoys, msg));
        }));
    }


    private static void ProcessMessage()
    {
        CancellationTokenSource cts = new();
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true; // prevent the process from terminating.
            cts.Cancel();
        };

        Task.Run(() =>
        {
            while (!cts.Token.IsCancellationRequested)
            {
                if (!_messages.Any())
                    continue;

                var value = _messages.Dequeue();

                switch (value.topic)
                {
                    case TopicBoys:
                    {
                        Console.WriteLine($"Received ♂boy♂ name: {value.message}");
                        break;
                    }
                    case TopicGirls:
                    {
                        Console.WriteLine($"Received girl name: {value.message}");
                        break;
                    }
                }
            }
        }, cts.Token);
    }
}