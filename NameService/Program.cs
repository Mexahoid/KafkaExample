
using Confluent.Kafka;
using MessageBroker;

namespace NameService;

class Program
{
   /* private static readonly string userHelpMsg = "NameService.\nEnter 'b' or 'g' to process boy or girl names respectively";
    private static readonly string bTopicNameCmd = "b_name_command";
    private static readonly string gTopicNameCmd = "g_name_command";

    private static readonly string bTopicNameResp = "b_name_response";
    private static readonly string gTopicNameResp = "g_name_response";
    private static readonly string topicCmdName = "get_name_request";
    private static readonly string topicRespName = "resp_name_request";
    private static MessageBus msgBus;*/
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
    };/*
    private static readonly string topicGirls = "girls";
    private static readonly string topicBoys = "gachi";
    */
    static void Main(string[] args)
    {
        Console.WriteLine("Awaiting");

        MessageBus2<string, string>.Instance.Consume("sas");


        /*bool canceled = false;

        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            canceled = true;
        };

        using (msgBus = new MessageBus())
        {
            Console.WriteLine(userHelpMsg);

            HandleUserInput();
            
        }*/

        
        
        /*CancellationTokenSource cts = new();
        var guid = MessageBus2<Null, string>.Instance.Subscribe(topicCmdName, Parse, cts);



        void Parse(string cmd)
        {
            switch (cmd)
            {
                case "get_boy_name":
                    MessageBus2.Instance.SendMessage(topicBoys, GetRandomCollegeBoy());
                    break;
                case "get_girl_name":
                    MessageBus2.Instance.SendMessage(topicGirls, GetRandomGirl());
                    break;
                case "quit":
                    Console.WriteLine("Quitting");
                    cts.Cancel();
                    //MessageBus2.Instance.Unsubscribe(guid, cts);
                    break;
            }
        }

        while (!cts.IsCancellationRequested)
        {

        }
        MessageBus2.Instance.Unsubscribe(guid, cts);*/
    }


    /*private static void HandleUserInput()
    {
        string? userInput;
        do
        {
            userInput = Console.ReadLine();
            switch (userInput)
            {
                case "b":
                    Task.Run(() =>
                        msgBus!.SubscribeOnTopic<string>(bTopicNameCmd, BoyNameCommandListener, CancellationToken.None));
                    Console.WriteLine("Processing boy names");
                    break;
                case "g":
                    Task.Run(() =>
                        msgBus!.SubscribeOnTopic<string>(gTopicNameCmd, GirlNameCommandListener,
                            CancellationToken.None));
                    Console.WriteLine("Processing girl names");
                    break;
                case "q":
                    Console.WriteLine("Exiting.");
                    break;
                default:
                    Console.WriteLine($"Unknown command. {userHelpMsg}");
                    continue;
            }

        } while (userInput != "q");
    }

    private static void BoyNameCommandListener(string msg)
    {
        var r = new Random().Next(0, 5);
        var randName = _boyNames[r];

        msgBus.SendMessage(bTopicNameResp, randName);
        Console.WriteLine($"Sending {randName}");
    }

    private static void GirlNameCommandListener(string msg)
    {
        var r = new Random().Next(0, 5);
        var randName = _girlNames[r];

        msgBus.SendMessage(gTopicNameResp, randName);
        Console.WriteLine($"Sending {randName}");
    }*/
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