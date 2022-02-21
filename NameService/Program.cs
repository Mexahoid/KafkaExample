
using System;
using Confluent.Kafka;
using MessageBroker;

namespace NameService;

class Program
{
    private static readonly string[] _boyNames =
    {
        "Arsenii",
        "Igor",
        "Kostya",
        "Ivan",
        "Dmitrii",
    };
    private static readonly string[] _girlNames =
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

    private static readonly MessageBus2<string> MsgBus = MessageBus2<string>.Instance;

    private static Queue<string> _messages = null!;

    static async Task CreateTopic(string topic)
    {
        Console.WriteLine($"Not found {topic}, trying to create");
        try
        {
            await MessageBus2<string>.Instance.CreateTopic(topic);
        }
        catch (Exception e)
        {
            Console.WriteLine(e.Message);
        }
    }

    static async Task Main(string[] args)
    {
        Console.WriteLine("Initializing..");
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

        _messages = new Queue<string>();

        CancellationTokenSource cts = new();
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true; // prevent the process from terminating.
            cts.Cancel();
        };


        Task.Run(() => ProcessMessage(cts.Token), cts.Token).GetAwaiter().GetResult();


        Console.WriteLine("Awaiting");


        await MsgBus.ConsumeContinuously(TopicCmdName, value =>
        {
            Console.WriteLine($"Got message from '{TopicCmdName}': {value}");
            _messages.Enqueue(value);
        });


    }

    private static void ProcessMessage(CancellationToken ct)
    {
        Task.Run(async () =>
        {
            while (!ct.IsCancellationRequested)
            {
                if (!_messages.Any())
                    continue;


                var value = _messages.Dequeue();

                switch (value)
                {
                    case "get_boy_name":
                    {
                        string name = GetRandomCollegeBoy();
                        Console.WriteLine($"Received boy command, sending name '{name}'");
                        await MsgBus.SendMessageAsync(TopicBoys, name, ct);
                        break;
                    }
                    case "get_girl_name":
                    {
                        string name = GetRandomGirl();
                        Console.WriteLine($"Received girl command, sending name '{name}'");
                        await MsgBus.SendMessageAsync(TopicGirls, name, ct);
                        //msgBus.SendMessage(TopicGirls, name);
                        break;
                    }
                }
            }
        }, ct);
    }


    private static string GetRandomCollegeBoy()
    {
        var r = new Random().Next(0, 5);
        var randName = _boyNames[r];
        Console.WriteLine($"Sending college boy: {randName}");
        return randName;
    }

    private static string GetRandomGirl()
    {
        var r = new Random().Next(0, 5);
        var randName = _girlNames[r];

        Console.WriteLine($"Sending girl name: {randName}");
        return randName;
    }
}