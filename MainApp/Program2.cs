using MessageBroker;

namespace MainApp;

class Program2
{
    private static readonly string bTopicNameCmd = "b_name_command";
    private static readonly string gTopicNameCmd = "g_name_command";
    private static readonly string bMessageReq = "get_boy_name";
    private static readonly string gMessageReq = "get_girl_name";
    private static readonly string bTopicNameResp = "b_name_response";
    private static readonly string gTopicNameResp = "g_name_response";

    private static readonly string userHelpMsg = "MainApp: Enter 'b' for a boy or 'g' for a girl, 'q' to exit";

    private static readonly string topicCmdName = "get_name_request";
    private static readonly string topicRespName = "resp_name_request";

    private static readonly string topicGirls = "girls";
    private static readonly string topicBoys = "gachi";

    static void Test(string msg)
    {
        Console.WriteLine("Lol: " + msg);
    }
    
    public static void GetBoyNameHandler(string msg)
    {
        Console.WriteLine($"Boy name {msg} is recommended");
    }

    public static void GetGirlNameHandler(string msg)
    {
        Console.WriteLine($"Girl name {msg} is recommended");
    }


    static void Mamin(string[] args)
    {
        //var msgBus = MessageBus2.Instance;

        /*msgBus.CreateTopic(topicCmdName).GetAwaiter().GetResult();
        msgBus.CreateTopic(topicBoys).GetAwaiter().GetResult();
        msgBus.CreateTopic(topicGirls).GetAwaiter().GetResult();

        CancellationTokenSource cts = new();
        var guid = MessageBus2.Instance.Subscribe(topicGirls, GetGirlNameHandler, cts);
        CancellationTokenSource cts2 = new();
        var guid2 = MessageBus2.Instance.Subscribe(topicBoys, GetBoyNameHandler, cts2);
        


        string? userInput;

        do
        {
            userInput = Console.ReadLine();
            switch (userInput)
            {
                case "b":
                    msgBus.SendMessage(topicCmdName, "get_boy_name");
                    //GetBoyNameHandler(msgBus.AwaitMessageAsync(topicRespName, CancellationToken.None));
                    break;
                case "g":
                    msgBus.SendMessage(topicCmdName, "get_girl_name"); 
                    //GetGirlNameHandler(msgBus.AwaitMessageAsync(topicRespName, CancellationToken.None));
                    break;
                case "q":
                    msgBus.SendMessage(topicCmdName, "quit");
                    MessageBus2.Instance.Unsubscribe(guid, cts);
                    MessageBus2.Instance.Unsubscribe(guid2, cts2);
                    break;
                default:
                    Console.WriteLine($"Unknown command. {userHelpMsg}");
                    break;
            }
        } while (userInput != "q");
        */

        /*Console.ReadLine();
        return;
        using (var msgBus = new MessageBus())
        {
            Task.Run(() => msgBus.SubscribeOnTopic<string>(bTopicNameResp, GetBoyNameHandler, CancellationToken.None));
            Task.Run(() => msgBus.SubscribeOnTopic<string>(gTopicNameResp, GetGirlNameHandler, CancellationToken.None));

            string userInput;

            do
            {
                Console.WriteLine(userHelpMsg);
                userInput = Console.ReadLine();
                switch (userInput)
                {
                    case "b":
                        msgBus.SendMessage(topic: bTopicNameCmd, message: bMessageReq);
                        break;
                    case "g":
                        msgBus.SendMessage(topic: gTopicNameCmd, message: gMessageReq);
                        break;
                    case "q":
                        break;
                    default:
                        Console.WriteLine($"Unknown command. {userHelpMsg}");
                        break;
                }

            } while (userInput != "q");
        }
        */
    }
}