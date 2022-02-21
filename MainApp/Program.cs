using System;
using MessageBroker;

namespace MainApp;

public class Program
{
    private static MessageBus2<string, string> _msgBus = MessageBus2<string, string>.Instance;
    static async Task Main(string[] args)
    {
        string word;

        Dictionary<string, (string description, Func<Task> command)> actions = new()
        {
            {"l", ("List all available topics", ListTopic)},
            {"c", ("Create topic", CreateTopics)},
            {"d", ("Delete topic", DeleteTopic)},
            {"s", ("Send message to topic", SendMessageToTopic)},
            {"r", ("Receive message from topic", ReceiveMessageFromTopic)},
            {"q", ("Quit program", () => Task.CompletedTask)}
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

            if (!actions.ContainsKey(word))
                Console.WriteLine("! Unknown command, try again. !");
            else
                await actions[word].command();

        } while (word != "q");

        
        //msgBus.SendMessage("sasiska", "sas", "sos");

        
    }

    static List<string> GetPrintTopics()
    {
        var topics = _msgBus.GetTopics();

        Console.WriteLine("# Available topics: #");
        for (var i = 0; i < topics.Count; i++)
        {
            Console.WriteLine($"{i + 1}. {topics[i]}");
        }

        return topics;
    }

    static Task ListTopic()
    {
        _ = GetPrintTopics();

        return Task.CompletedTask;
    }

    static async Task CreateTopics()
    {
        await CreateDelete(true);
    }


    private static async Task CreateDelete(bool isCreating)
    {
        var topics = new List<string>();
        Func<string, Task> action = isCreating ? _msgBus.CreateTopic : _msgBus.DeleteTopic;
        var word = isCreating ? "Creat" : "Delet";

        Console.WriteLine($"# {word}ing topic");
        string topic;
        do
        {
            if (!isCreating)
                topics = GetPrintTopics();


            Console.WriteLine(isCreating ? "Type topic name to create." : "Type topic number to delete.");
            Console.WriteLine($"Or type 'q' to quit without {word.ToLower()}ing.");

            Console.Write(">> ");

            if (isCreating)
            {
                topic = Console.ReadLine()!;
            }
            else
            {
                int number;
                while (!int.TryParse(Console.ReadLine(), out number) || number < 1 || number > topics.Count)
                {
                    Console.WriteLine("Try again, bad number.");
                    Console.Write(">> ");
                }

                topic = topics[number - 1];
            }
            

            try
            {
                await action(topic);
                Console.WriteLine($"Successfully {word.ToLower()}ed topic '{topic}'.");
                return;
            }
            catch (Exception e)
            {
                Console.WriteLine($"Error: {e.Message}");
            }
        } while (topic != "q");
        Console.WriteLine($"Quitting without {word.ToLower()}ing.");
    }



    static Task DeleteTopic()
    {
        Console.WriteLine("Here used to be delete topic function but due to Kafka problems on Windows it's turned off :(");
        return Task.CompletedTask;

        /*
        await CreateDelete(false);
*/
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

        _msgBus.SendMessage(topic, message!.GetHashCode().ToString(), message);
        return Task.CompletedTask;
    }


    static Task ReceiveMessageFromTopic()
    {
        Console.WriteLine("# Receive message #");

        var topic = string.Empty;

        while (string.IsNullOrEmpty(topic))
        {
            Console.Write("Type in topic name >> ");
            topic = Console.ReadLine();
        }
        

        _msgBus.Consume(topic);
        return Task.CompletedTask;
    }
}