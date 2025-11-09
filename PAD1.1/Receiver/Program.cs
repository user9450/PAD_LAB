using System.Net.Sockets;
using System.Text;
using System.Text.Json;

class Receiver
{
    static void Main()
    {
        string brokerAddress = "127.0.0.1";
        int brokerPort = 10000;

        Console.Write("Introduceti subiectele (separate prin virgula): ");
        string input = Console.ReadLine().Trim();
        var subjects = input.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

        Console.Write("Introduceti identificatorul unic: ");
        string id = Console.ReadLine().Trim();

        using var client = new TcpClient(brokerAddress, brokerPort);
        using var stream = client.GetStream();
        using var writer = new StreamWriter(stream, Encoding.UTF8) { AutoFlush = true };
        using var reader = new StreamReader(stream, Encoding.UTF8);
        
        var initMessageObj = new
        {
            role = "RECEIVER",
            id,
            subjects
        };

        string initMessage = JsonSerializer.Serialize(initMessageObj);
        writer.WriteLine(initMessage);
        Console.WriteLine("Trimis la Broker: " + initMessage);
        
        Thread listenThread = new Thread(() =>
        {
            try
            {
                string line;
                Console.WriteLine("Se asteapta mesaje pentru subiectele: " + string.Join(", ", subjects));
                while ((line = reader.ReadLine()) != null)
                {
                    Console.WriteLine("Mesaj primit: " + line);
                }
            }
            catch
            {
                Console.WriteLine("Conexiunea cu Broker-ul s-a intrerupt.");
            }
        });

        listenThread.Start();
        
        listenThread.Join();
        while (true)
        {
            Thread.Sleep(1000);
        }
        
    }
}