using Confluent.Kafka;

namespace MessageBroker
{
    public class MessageBus : IDisposable
    {
        private readonly IDictionary<string, string> _consumerConfig;

        private readonly IProducer<Null, string> _producer;
        private IConsumer<Null, string>? _consumer;

        public MessageBus() : this("localhost")
        {

        }

        public MessageBus(string host)
        {
            IDictionary<string, string> producerConfig = new Dictionary<string, string>
            {
                {"bootstrap.servers", host}
            };

            _consumerConfig = new Dictionary<string, string>
            {
                {"group.id", "custom-group"},
                {"bootstrap.servers", host}
            };

            _producer = new ProducerBuilder<Null, string>(producerConfig).Build();

        }

        public void SendMessage(string topic, string message)
        {
            var msg = new Message<Null, string>
            {
                Value = message
            };
            _producer.ProduceAsync(topic, msg);
        }


        public void SubscribeOnTopic<T>(string topic, Action<T> action, CancellationToken cancellationToken) where T : class
        {
            var msgBus = new MessageBus();
            using (msgBus._consumer = new ConsumerBuilder<Null, string>(_consumerConfig).Build())
            {
                msgBus._consumer.Assign(new List<TopicPartitionOffset>
                {
                    new(topic, 0, -1)
                });

                while (!cancellationToken.IsCancellationRequested)
                {
                    var xx = msgBus._consumer.Consume(TimeSpan.FromMilliseconds(10));

                    if (xx?.Message.Value is T value)
                    {
                        action(value);
                    }

                }

            }

        }

        public void Dispose()
        {
            _producer?.Dispose();
            _consumer?.Dispose();
        }
    }
}