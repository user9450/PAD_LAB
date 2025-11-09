using Broker;
using Grpc.Core;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.AspNetCore.Hosting;
using System.Collections.Concurrent;

// Dicționar care mapează un ID de client la coada lui de mesaje
var clientQueues = new ConcurrentDictionary<string, ConcurrentQueue<Message>>();

// Stocare globală a mesajelor, grupate după topic
var topicHistory = new ConcurrentDictionary<string, List<Message>>();

var builder = WebApplication.CreateBuilder(args);

// Configurare server Kestrel pentru rulare pe HTTP/2 (port 5000)
builder.WebHost.ConfigureKestrel(opts =>
{
    opts.ListenLocalhost(5000, cfg => cfg.Protocols = HttpProtocols.Http2);
});

// Înregistrăm serviciile necesare pentru gRPC
builder.Services.AddGrpc();
builder.Services.AddSingleton(clientQueues);
builder.Services.AddSingleton(topicHistory);

var app = builder.Build();

// Atașăm serviciul gRPC implementat
app.MapGrpcService<ReworkedBrokerService>();
app.MapGet("/", () => "Broker gRPC online pe portul 5000 (HTTP/2)");

app.Run();

public class ReworkedBrokerService : BrokerService.BrokerServiceBase
{
    private readonly ConcurrentDictionary<string, ConcurrentQueue<Message>> _queues;
    private readonly ConcurrentDictionary<string, List<Message>> _history;

    public ReworkedBrokerService(
        ConcurrentDictionary<string, ConcurrentQueue<Message>> queues,
        ConcurrentDictionary<string, List<Message>> history)
    {
        _queues = queues;
        _history = history;
    }

    public override Task<Ack> SendMessage(Message request, ServerCallContext context)
    {
        // Trimitem fiecare mesaj tuturor clienților abonați
        foreach (var q in _queues.Values)
        {
            q.Enqueue(request);
        }

        // Salvăm mesajul în istoricul topic-ului aferent
        var topicList = _history.GetOrAdd(request.Subject, _ => new List<Message>());
        lock (topicList)
        {
            topicList.Add(request);
        }

        Console.WriteLine($"[Broker] Mesaj primit => {request.Subject}: {request.Content}");
        return Task.FromResult(new Ack { Success = true });
    }

    public override async Task Subscribe(
        SubscribeRequest request,
        IServerStreamWriter<Message> stream,
        ServerCallContext context)
    {
        var queue = _queues.GetOrAdd(request.ReceiverId, _ => new ConcurrentQueue<Message>());

        Console.WriteLine($"[Broker] Clientul {request.ReceiverId} urmărește topicul '{request.Subject}'.");

        // Trimitem istoricul topicului dacă există
        if (_history.TryGetValue(request.Subject, out var oldMessages))
        {
            List<Message> localCopy;
            lock (oldMessages)
            {
                localCopy = new List<Message>(oldMessages);
            }

            foreach (var msg in localCopy)
            {
                await stream.WriteAsync(msg);
                Console.WriteLine($"[Broker] Istoric -> {request.ReceiverId}: {msg.Subject}: {msg.Content}");
            }
        }

        // Ascultăm mesaje noi
        while (!context.CancellationToken.IsCancellationRequested)
        {
            if (queue.TryDequeue(out var msg))
            {
                if (msg.Subject == request.Subject)
                {
                    await stream.WriteAsync(msg);
                    Console.WriteLine($"[Broker] Trimis către {request.ReceiverId}: {msg.Subject}: {msg.Content}");
                }
            }
            else
            {
                await Task.Delay(500);
            }
        }
    }
}
