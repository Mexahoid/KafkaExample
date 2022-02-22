using System.Threading.Channels;
using MessageBroker;
using Newtonsoft.Json;

namespace MainApp;

public class Program
{
    static async Task Main()
    {
        var msgs = new Queue<string?>();
        var mh = new MessageHost(msgs, Console.WriteLine);

        CancellationTokenSource cts2 = new();
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true; // prevent the process from terminating.
            cts2.Cancel();
        };

#pragma warning disable CS4014
        Task.Run(() =>
#pragma warning restore CS4014
        {
            while (!cts2.IsCancellationRequested)
            {
                msgs.Enqueue(Console.ReadLine());
            }
        }, cts2.Token);

        await mh.StartAsync(cts2.Token);
        
    }
}