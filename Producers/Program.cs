// See https://aka.ms/new-console-template for more information
using Confluent.Kafka;

Console.WriteLine("Hello, World!");

var config = new ProducerConfig { BootstrapServers = "localhost:29092" };
using (var producer = new ProducerBuilder<Null, string>(config).Build())
{
    try
    {
        var dr = await producer.ProduceAsync("my-topic", new Message<Null, string> { Value = "hello kociks" });
        Console.WriteLine($"Delivered '{dr.Value}' to '{dr.TopicPartitionOffset}'");
    }
    catch (ProduceException<Null, string> e)
    {
        Console.WriteLine($"Delivery failed: {e.Error.Reason}");
    }
}