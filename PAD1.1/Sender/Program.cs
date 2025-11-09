using System.Net.Sockets;
using System.Text;
using System.Text.Json;

class Sender
{
    static void Main()
    {
        string brokerAddress = "127.0.0.1";
        int brokerPort = 10000;
        TcpClient client = null;
        
        while (client == null)
        {
            try
            {
                client = new TcpClient();
                client.Connect(brokerAddress, brokerPort);
                Console.WriteLine("Conectarea la Broker: {0}:{1}", brokerAddress, brokerPort);
            }
            catch
            {
                Console.WriteLine("Broker-ul e deconectat, asteptati 3 secunde...");
                Thread.Sleep(3000);
                client = null;
            }
        }

        using (var stream = client.GetStream())
        using (var writer = new StreamWriter(stream, Encoding.UTF8) { AutoFlush = true })
        {
            // Handshake
            writer.WriteLine("{ \"role\": \"SENDER\" }");

            while (true)
            {
                Console.Write("subject: ");
                string subject = Console.ReadLine().Trim();

                Console.Write("text: ");
                string text = Console.ReadLine().Trim();

                var message = new
                {
                    id = Guid.NewGuid().ToString(),
                    subject,
                    text,
                    time = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")
                };

                string json = JsonSerializer.Serialize(message);
                writer.WriteLine(json);

                Console.WriteLine("Mesajul:\n" + json);
            }
        }
    }
}