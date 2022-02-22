using MessageBroker;
using Newtonsoft.Json;

namespace MainApp;

public class MessageHost
{
    private readonly MessageBus<string, string> _msgBus;

    private const string TopicCmdName = "requests";
    private const string TopicGirls = "girls";
    private const string TopicBoys = "gachi";
    private const string TopicNumbers = "numbers";

    private readonly Producer<string, string> _producer;
    private CancellationToken _ct;
    
    private readonly List<Subscriber<string, string>> _subscribers;

    private readonly Queue<string?> _inputQueue;

    private readonly Action<string> _logger;



    public MessageHost(Queue<string?> inputQueue, Action<string> logger)
    {
        _logger = logger;
        _msgBus = new MessageBus<string, string>(logger);
        _inputQueue = inputQueue;

        _producer = _msgBus.GetProducer();

        _subscribers = new List<Subscriber<string, string>>();
    }

    public async Task StartAsync(CancellationToken ct)
    {
        _ct = ct;
        await _msgBus.EnsureTopicsCreated(TopicCmdName, TopicNumbers, TopicBoys, TopicGirls);
        lock (_subscribers)
        {
            _inputQueue.Clear();
        }

        Dictionary<string, (string description, Func<Task> command)> actions = new()
        {
            { "b", ("Get boy name", GetBoyName) },
            { "g", ("Get girl name", GetGirlName) },
            { "n", ("Get random numbers", GetNumbers) },
            { "q", ("Quit program", Quit) }
        };

        _subscribers.Add(_msgBus.GetSubscriber(TopicBoys, "names", (k, msg) =>
        {
            OnLogger($"Received ♂boy♂ name: {msg}");
        }, _ct));

        _subscribers.Add(_msgBus.GetSubscriber(TopicGirls, "names", (k, msg) =>
        {
            OnLogger($"Received girl name: {msg}");
        }, _ct));

        _subscribers.Add(_msgBus.GetSubscriber(TopicNumbers, "numbers", (k, msg) =>
        {
            if (string.IsNullOrEmpty(msg))
                return;
            var lst = JsonConvert.DeserializeObject<List<int>>(msg);
            if (lst == null)
                return;

            OnLogger($"Got {lst.Count} numbers:");
            for (var i = 0; i < lst.Count; i++)
            {
                OnLogger($"{i + 1}. {lst[i]}");
            }
        }, _ct));


        await Task.Run(async () =>
        {
            bool fired = false;
            while (!ct.IsCancellationRequested)
            {
                if (!fired)
                {
                    OnLogger("# Available commands: #");
                    foreach (var key in actions.Keys)     
                        OnLogger($"'{key}' - {actions[key].description}");
                    fired = true;
                }
                bool none;
                lock (_subscribers)
                {
                    none = !_inputQueue.Any();
                }
                if (none)
                    continue;

                fired = false;
                string? word;
                lock (_subscribers)
                {
                    word = _inputQueue.Dequeue();
                }

                if (string.IsNullOrEmpty(word))
                    continue;
                if (!actions.ContainsKey(word))
                    OnLogger("! Unknown command, try again. !");
                else
                    await actions[word].command();
            }
        }, ct);
    }

    private async Task GetBoyName()
    {
        OnLogger("# Requesting ♂college boy♂.. #");
        const string message = "get_boy_name";
        await _producer.SendMessageAsync(TopicCmdName, message, string.Empty, _ct);
    }

    private async Task GetNumbers()
    {
        OnLogger("# Requesting random numbers.. #");
        const string message = "get_numbers";
        int number;
        string? input;
        do
        {
            OnLogger("Type number of numbers (more than 0).");


            bool none;
            do
            {
                lock (_subscribers)
                {
                    none = !_inputQueue.Any();
                }
            } while (none);


            lock (_subscribers)
            {
                input = _inputQueue.Dequeue();
            }
        } while (!int.TryParse(input, out number) && number < 1);

        await _producer.SendMessageAsync(TopicCmdName, message, number.ToString(), _ct);
    }

    private async Task GetGirlName()
    {
        OnLogger("# Requesting random girl name.. #");
        const string message = "get_girl_name";
        await _producer.SendMessageAsync(TopicCmdName, message, string.Empty, _ct);
    }

    private async Task Quit()
    {
        OnLogger("# Sending quit message.. #");
        const string message = "quit";
        await _producer.SendMessageAsync(TopicCmdName, message, string.Empty, _ct);
    }

    protected virtual void OnLogger(string obj)
    {
        _logger(obj);
    }
}