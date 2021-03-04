using System;
using System.Threading;
using System.Threading.Tasks;
using ActiveMQ.Artemis.Client;
using ActiveMQ.Artemis.Client.Exceptions;
using Microsoft.Extensions.Logging;

namespace Producer
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, _) => { cts.Cancel(); };

            using var loggerFactory = LoggerFactory.Create(builder =>
            {
                builder.SetMinimumLevel(LogLevel.Information);
                builder.AddConsole();
            });

            var connectionFactory = new ConnectionFactory
            {
                LoggerFactory = loggerFactory
            };
            var endpoint = Endpoint.Create(host: "localhost", port: 5672, "guest", "guest");

            await using var connection = await connectionFactory.CreateAsync(endpoint, cts.Token);
            connection.ConnectionRecoveryError += (_, _) => { cts.Cancel(); };

            var address = "MyAddress";

            var producer = await connection.CreateProducerAsync(address, RoutingType.Multicast, cts.Token);

            int counter = 1;
            while (!cts.IsCancellationRequested)
            {
                try
                {
                    await producer.SendAsync(new Message(counter.ToString()), cancellationToken: cts.Token);
                    Console.WriteLine($"Message sent: {counter}");
                    counter++;
                    await Task.Delay(TimeSpan.FromSeconds(1), cts.Token);
                }
                catch (OperationCanceledException)
                {
                }
                catch (ActiveMQArtemisClientException)
                {
                }
            }
        }
    }
}