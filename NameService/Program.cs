
using System;
using Confluent.Kafka;
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

    private static readonly Producer<string> Prod = new();

    private static readonly CancellationTokenSource Cts = new();
    private static readonly MessageBus<string> MsgBus = MessageBus<string>.Instance;


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
        Console.WriteLine("Initializing..");
        await Prerequisites();


        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true; // prevent the process from terminating.
            Cts.Cancel();
        };

        Subscriber<string> sub = new(TopicCmdName, ParseAction, Cts.Token);
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

    private static async void ParseAction(string value)
    {
        switch (value)
        {
            case "get_boy_name":
            {
                string name = GetRandomCollegeBoy();
                Console.WriteLine($"Received boy command, sending name '{name}'");
                await Prod.SendMessageAsync(TopicBoys, name);
                break;
            }
            case "get_girl_name":
            {
                string name = GetRandomGirl();
                Console.WriteLine($"Received girl command, sending name '{name}'");
                await Prod.SendMessageAsync(TopicGirls, name);
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