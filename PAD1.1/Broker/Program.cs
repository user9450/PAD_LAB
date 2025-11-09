using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Collections.Concurrent;

class Broker
{
    static void Main()
    {
        int brokerPort = 10000;
        
        var receivers = new ConcurrentDictionary<TcpClient, string[]>();
        
        var messageBuffer = new ConcurrentQueue<string>();

        TcpListener server = new TcpListener(IPAddress.Any, brokerPort);
        server.Start();
        Console.WriteLine("Broker pornit pe portul: " + brokerPort);

        while (true)
        {
            TcpClient client = server.AcceptTcpClient();
            Console.WriteLine("Client nou: " + client.Client.RemoteEndPoint);

            ThreadPool.QueueUserWorkItem(_ => HandleClient(client, receivers, messageBuffer));
        }
    }

    static void HandleClient(
        TcpClient client,
        ConcurrentDictionary<TcpClient, string[]> receivers,
        ConcurrentQueue<string> messageBuffer)
    {
        using var stream = client.GetStream();
        using var reader = new StreamReader(stream, Encoding.UTF8);
        using var writer = new StreamWriter(stream, Encoding.UTF8) { AutoFlush = true };

        try
        {
            string firstLine = reader.ReadLine();
            var doc = JsonDocument.Parse(firstLine);
            var role = doc.RootElement.GetProperty("role").GetString();

            // RECEIVER 
            if (role == "RECEIVER")
            {
                var subjectsArray = doc.RootElement.GetProperty("subjects")
                                                  .EnumerateArray()
                                                  .Select(x => x.GetString())
                                                  .ToArray();

                receivers[client] = subjectsArray;
                Console.WriteLine("Receiver conectat pentru subiecte: " + string.Join(", ", subjectsArray));

                // Ține clientul conectat
                while (client.Connected)
                    Thread.Sleep(1000);
            }
            // SENDER
            else if (role == "SENDER")
            {
                Console.WriteLine("Sender conectat");

                string line;
                while ((line = reader.ReadLine()) != null)
                {
                    Console.WriteLine("Mesaj primit de la Sender: " + line);
                    
                    messageBuffer.Enqueue(line);
                    
                    var msgDoc = JsonDocument.Parse(line);
                    string messageSubject = msgDoc.RootElement.GetProperty("subject").GetString();
                    
                    foreach (var kvp in receivers)
                    {
                        var receiverClient = kvp.Key;
                        var receiverSubjects = kvp.Value;

                        if (receiverSubjects.Contains(messageSubject))
                        {
                            try
                            {
                                using var rWriter = new StreamWriter(receiverClient.GetStream(), Encoding.UTF8, leaveOpen: true) { AutoFlush = true };
                                rWriter.WriteLine(line);
                            }
                            catch
                            {
                                Console.WriteLine("Nu s-a putut trimite mesajul către un Receiver.");
                            }
                        }
                    }
                }

                Console.WriteLine("Sender s-a deconectat.");
            }
            else
            {
                Console.WriteLine("Client necunoscut, deconectare");
                client.Close();
            }
        }
        catch (Exception e)
        {
            Console.WriteLine("Eroare client: " + e.Message);
        }
        finally
        {
            receivers.TryRemove(client, out _);
            client.Close();
        }
    }
}
