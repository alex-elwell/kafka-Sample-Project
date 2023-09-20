using Confluent.Kafka; 
using Microsoft.Extensions.Hosting;

namespace kafka;

public class SendData : IHostedService
{
    // send data to kafka cluster
    private bool _shouldRun = true;
    public Task Connect()
    {
        var config = new ProducerConfig
        {
            BootstrapServers = "0.0.0.0:29092",
        };
        
        using (var producer = new ProducerBuilder<Null, string>(config).Build())
        {
            if (_shouldRun)
            {
                for (int i = 0; i < 100; ++i)
                {
                    var value = $"data-{i}";
                    Console.WriteLine($"Producing record: {value}");
                    producer.Produce("your_topic_name", new Message<Null, string> { Value = value },
                        (deliveryReport) =>
                        {
                            Console.WriteLine(
                                $"Partition: {deliveryReport.Partition}, Offset: {deliveryReport.Offset}");
                        });
                }
            }
            // wait for up to 10 seconds for any inflight messages to be delivered.
            producer.Flush(TimeSpan.FromSeconds(10));
        }
        return Task.CompletedTask; 
    }
    public Task StartAsync(CancellationToken cancellationToken)
    {
        return Connect();
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        _shouldRun = false;
        return Task.CompletedTask;
    }
}