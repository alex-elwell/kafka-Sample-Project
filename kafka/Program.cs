using System.Data;
using kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
 
namespace kafka 
{
    class kafka
    {
        static void Main(string[] args)
        {
            ReceiveData receiveData = new ReceiveData();
            SendData sendData = new SendData();
            sendData.Connect();
            receiveData.receive();
            var host = new HostBuilder()
                .ConfigureServices((hostContext, services) =>
                {
                    // Register your hosted service
                    services.AddHostedService<ReceiveData>();
                    services.AddHostedService<SendData>();
                })
                .Build();
            
            host.Run();
        }
    }
}