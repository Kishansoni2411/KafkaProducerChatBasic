using Confluent.Kafka; // Import the Kafka library
using System; // Import system utilities
using System.Threading.Tasks; // Import task utilities

namespace KafkaProducerApp // Define the namespace
{
    class Program // Define the main Program class
    {
        private static readonly string _bootstrapServers = "localhost:9092"; // Kafka server address
        private static readonly string _topic = "simple-messages"; // Kafka topic name

        static async Task Main(string[] args) // Entry point of the program
        {
            Console.WriteLine("Kafka Producer Application");
            Console.WriteLine("Press 'q' to quit or enter a message to send:");

            while (true)
            {
                var input = Console.ReadLine(); // Read input from the user
                if (input?.ToLower() == "q") // If the input is 'q', exit the loop
                {
                    break;
                }

                await ProduceAsync(input); // Send the input message to Kafka
            }
        }

        // Async method to send a message to Kafka
        public static async Task ProduceAsync(string message)
        {
            // Create a configuration for the Kafka producer
            var config = new ProducerConfig { BootstrapServers = _bootstrapServers };

            // Create a new producer with the given configuration
            using var producer = new ProducerBuilder<Null, string>(config).Build();

            try
            {
                // Send a message to the specified topic
                var result = await producer.ProduceAsync(_topic, new Message<Null, string> { Value = message });
                // Print the result of the delivery
                Console.WriteLine($"Delivered '{result.Value}' to '{result.TopicPartitionOffset}'");
            }
            catch (ProduceException<Null, string> e) // Catch any errors
            {
                // Print the error message if delivery fails
                Console.WriteLine($"Delivery failed: {e.Error.Reason}");
            }
        }
    }
}
