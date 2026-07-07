using StackExchange.Redis;

namespace YellowMacaroni.Redis.Queue
{
    public class QueueClient
    {
        private ConnectionMultiplexer _connection;
        private int _retryCount = 0;

        private int MAX_WAIT_TIME_SECONDS = 20;

        public event EventHandler Connecting;
        public event EventHandler Connected;
        public event EventHandler<Exception> ConnectionError;

        private void Connect(string connectionString)
        {
            try
            {
                Connecting?.Invoke(this, EventArgs.Empty);
                _connection = ConnectionMultiplexer.Connect(connectionString);
                Connected?.Invoke(this, EventArgs.Empty);
                _retryCount = 0;
            }
            catch (Exception ex)
            {
                _retryCount++;
                ConnectionError?.Invoke(this, ex);

                // Exponential backoff with a maximum wait time of MAX_WAIT_TIME_SECONDS seconds
                Thread.Sleep((Math.Min(_retryCount * _retryCount, MAX_WAIT_TIME_SECONDS)) * 1000);

                Connect(connectionString);
            }
        }

#pragma warning disable CS8618
        public QueueClient(string connectionString)
        {
            Connect(connectionString);
        }

        public QueueClient(ConfigurationOptions options)

        {
            Connect(options.ToString());
        }
#pragma warning restore CS8618

        public IDatabase GetDatabase(int db = -1)
        {
            return _connection.GetDatabase(db);
        }

        public ISubscriber GetSubscriber()
        {
            return _connection.GetSubscriber();
        }
    }
}
