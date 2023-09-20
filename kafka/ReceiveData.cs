using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
namespace kafka;

public class ReceiveData : IHostedService
{
    private bool _shouldRun = true;
    public Task receive()
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = "0.0.0.0:29092",
            GroupId = "your_topic_name"
        };
        string topic = "your_topic_name";
        
        // receive the data from kafka cluster
        using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
        {
            consumer.Subscribe(topic);
            try
            {
                while (_shouldRun)
                {
                    var cr = consumer.Consume();
                    Console.WriteLine($"Consumed message '{cr.Value}' at: '{cr.TopicPartitionOffset}'.");
                }
            }
            catch (ConsumeException e)
            {
                Console.WriteLine($"Error occured: {e.Error.Reason}");
            }
            finally
            {
                consumer.Close();
            }
        }
        return Task.CompletedTask;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        receive();
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        _shouldRun = false;
        return Task.CompletedTask;
    }
}