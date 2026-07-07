using Newtonsoft.Json;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Text;

#pragma warning disable CS0618
namespace YellowMacaroni.Redis.Queue
{
    public class RedisQueue(QueueClient client, string name, QueueOptions? options = null) : RedisQueue<object>(client, name, options)
    { }
    public class RedisQueue<T>
    {
        private readonly IDatabase _database;
        private readonly ISubscriber _subscriber;
        public readonly string machineId;
        public readonly string groupName;
        public readonly string name;

        private readonly QueueOptions? options;

        public RedisQueue(QueueClient client, string name, QueueOptions? options = null)
        {
            this._database = client.GetDatabase();
            this._subscriber = client.GetSubscriber();
            this.machineId = options?.MachineId ?? Guid.NewGuid().ToString();
            this.groupName = options?.GroupName ?? name;
            this.name = name;
            this.options = options;

            try
            {
                _database.StreamCreateConsumerGroup(name, groupName, "0-0", true);
            }
            catch { }
        }

        /// <summary>
        /// Enqueues a message of type <typeparamref name="T"/> into the Redis stream. The message is serialized into JSON and stored in a field names "data" in the stream entry.
        /// </summary>
        /// <param name="data"></param>
        /// <returns></returns>
        public async Task<RedisValue> Enqueue(T data)
        {
            var result = await _database.StreamAddAsync(name,
            [
                new NameValueEntry("id", Guid.NewGuid().ToString()),
                new NameValueEntry("data", JsonConvert.SerializeObject(data)),
                new NameValueEntry("attempt", "1"),
                new NameValueEntry("timestamp", DateTimeOffset.UtcNow.ToUnixTimeMilliseconds().ToString())
            ]);

            if (options?.PublishEvents ?? true)
            {
                Task _ = _subscriber.PublishAsync($"yellowmacaroni.redis.queue-{name}", "new_message");
            }

            return result;
        }

        public async Task Enqueue(T data, TimeSpan runIn)
        {
            long readyAt = DateTimeOffset.UtcNow.Add(runIn).ToUnixTimeMilliseconds();

            var payload = new
            {
                id = Guid.NewGuid().ToString(),
                data,
                attempt = "1",
                timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds().ToString()
            };

            await _database.SortedSetAddAsync(
                $"queue:{name}:delayed", 
                JsonConvert.SerializeObject(payload),
                readyAt
            );
        }

        public async Task Requeue(StreamEntry entry)
        {
            if (entry.GetAttempt() >= (options?.MaxRetries ?? 3))
            {
                await AcknowledgeEntry(entry);
                return;
            }

            await _database.StreamAddAsync(name,
            [
                new NameValueEntry("id", entry.GetId()),
                new NameValueEntry("data", entry.GetData()),
                new NameValueEntry("attempt", (entry.GetAttempt() + 1).ToString()),
                new NameValueEntry("timestamp", DateTimeOffset.UtcNow.ToUnixTimeMilliseconds().ToString())
            ]);
            await AcknowledgeEntry(entry);
        }

        public async Task Requeue(StreamEntry entry, TimeSpan runIn)
        {
            if (entry.GetAttempt() >= (options?.MaxRetries ?? 3))
            {
                await AcknowledgeEntry(entry);
                return;
            }

            long readyAt = DateTimeOffset.UtcNow.Add(runIn).ToUnixTimeMilliseconds();
            var payload = new
            {
                id = entry.GetId(),
                data = entry.GetData(),
                attempt = (entry.GetAttempt() + 1).ToString(),
                timestamp = entry.GetUnixTimestampMs().ToString()
            };
            await _database.SortedSetAddAsync(
                $"queue:{name}:delayed",
                JsonConvert.SerializeObject(payload),
                readyAt
            );
            await AcknowledgeEntry(entry);
        }

        private const string _promoteScript = @"
            local due = redis.call('ZRANGEBYSCORE', KEYS[1], 0, ARGV[1])
            for _, member in ipairs(due) do
                redis.call('ZREM', KEYS[1], member)
                local job = cjson.decode(member)
                redis.call('XADD', KEYS[2], '*',
                    'id', job.id,
                    'data', job.data,
                    'attempt', job.attempt,
                    'timestamp', job.timestamp)
            end
            return #due";

        public async Task<int> RequeueDelayedJobs()
        {
            var result = await _database.ScriptEvaluateAsync(
                _promoteScript,
                [$"queue:{name}:delayed", name],
                [DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()]
            );

            return (int)result;
        }

        /// <summary>
        /// Enqueues a message of type <typeparamref name="T"/> into the Redis stream and waits for a response of type <typeparamref name="R"/>. The message is serialized into JSON and stored in a field names "data" in the stream entry. The method will wait for a response for up to <paramref name="timeout"/> seconds before returning null if no response is received.
        /// </summary>
        /// <typeparam name="R"></typeparam>
        /// <param name="data"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public async Task<R?> Enqueue<R>(T data, int timeout = 30)
        {
            string id = Guid.NewGuid().ToString();

            try
            {
                CancellationTokenSource cts = new(TimeSpan.FromSeconds(timeout));
                SemaphoreSlim semaphore = new(0, 1);
                R? returnData = default;

                // Listen for the response on a unique channel based on the message ID
                await _subscriber.SubscribeAsync($"yellowmacaroni.redis.queue-{name}-{id}", (_, value) =>
                {
                    if (cts.IsCancellationRequested) return;
                    returnData = JsonConvert.DeserializeObject<R>(value.ToString());
                    semaphore.Release();
                });

                await _database.StreamAddAsync(name,
                [
                    new NameValueEntry("id", id),
                    new NameValueEntry("data", JsonConvert.SerializeObject(data)),
                    new NameValueEntry("attempt", "1"),
                    new NameValueEntry("timestamp", DateTimeOffset.UtcNow.ToUnixTimeMilliseconds().ToString())
                ]);

                // Wait for the response or timeout
                await semaphore.WaitAsync(TimeSpan.FromMilliseconds(options?.ReturnMaxWaitMs ?? 30_000), cts.Token).ContinueWith(_ => { });

                return returnData;
            }
            finally
            {
                Task _ = _subscriber.UnsubscribeAsync($"yellowmacaroni.redis.queue-{name}-{id}"); // Cleanup subscription
            }
        }

        public async Task<StreamEntry[]> Dequeue(int count)
        {
            var result = await _database.StreamReadGroupAsync(
                name,
                groupName,
                consumerName: machineId,
                count: count,
                position: options?.GetQueuePositionString() ?? ">"
            );

            return result;
        }

        /// <summary>
        /// Fetches the next message from the queue stream from the current consumer group or null if there are no messages available.
        /// </summary>
        /// <returns></returns>
        public async Task<StreamEntry?> Dequeue()
        {
            await _database.StreamCreateConsumerGroupAsync(name, groupName, "0-0", true);

            var result = await Dequeue(1);            

            if (result.Length > 0)
            {
                return result[0];
            }
            else
            {
                return null;
            }
        }

        /// <summary>
        /// Listens for messages in the queue stream and invokes the provided callback whenever a message is recieved. This method will continuously poll the stream for new messages and will call the callback with the deserialized message.
        /// </summary>
        /// <param name="onMessageReceived"></param>
        /// <param name="sleepTimeMs"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        public async Task ListenForMessages(Action<StreamEntry> onMessageReceived, CancellationToken? ct = null)
        {
            SemaphoreSlim semaphore = new(0, 1);

            await _subscriber.SubscribeAsync($"yellowmacaroni.redis.queue-{name}", (_, _) =>
            {
                if (ct?.IsCancellationRequested ?? false) return;
                semaphore.Release();
            });

            while (!ct?.IsCancellationRequested ?? true)
            {
                await semaphore.WaitAsync(TimeSpan.FromMilliseconds(options?.PollWaitMs ?? 1000), ct ?? CancellationToken.None).ContinueWith(_ => { });

                while (true)
                {
                    var entries = await Dequeue(options?.DequeueCount ?? 10);

                    if (entries.Length == 0) break;

                    foreach (var entry in entries)
                    {
                        onMessageReceived(entry);
                    }
                }
            }

            await _subscriber.UnsubscribeAsync($"yellowmacaroni.redis.queue-{name}");
        }

        public async Task<long> Return(StreamEntry entry, object result)
        {
            return await _subscriber.PublishAsync(
                $"yellowmacaroni.redis.queue-{name}-{entry.Values.FirstOrDefault(x => x.Name == "id").Value}",
                JsonConvert.SerializeObject(result)
            );
        }

        /// <summary>
        /// Listens for messages in the queue stream and invokes the provided callback whenever a message is received. The callback is expected to return a response of type <typeparamref name="R"/> which will be serialized and stored in Redis for retrieval by the producer. This method will continuously poll the stream for new messages and will call the callback with the deserialized message.
        /// </summary>
        /// <typeparam name="R"></typeparam>
        /// <param name="onMessageReceived"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        public async Task ListenForMessagesWithCallback<R>(Func<StreamEntry, Task<QueueResponse<R>>> onMessageReceived, CancellationToken? ct = null) where R: class
        {
            await ListenForMessages(async (entry) =>
            {
                var result = await onMessageReceived(entry);
                if (result.data is null || result.shouldRetry)
                {
                    if (result.retryInMs >= 0)
                    {
                        await Requeue(entry, TimeSpan.FromMilliseconds(result.retryInMs));
                    }
                    else
                    {
                        await Requeue(entry, TimeSpan.FromMilliseconds(options?.GetBackoff(entry.GetAttempt()) ?? 1000));
                    }
                }
                else
                {
                    await Return(entry, result.data);
                    await AcknowledgeEntry(entry);
                }
            }, ct);
        }

        /// <summary>
        /// Deserializes the data from a StreamEntry into an object of type <typeparamref name="T"/>. This method assumes that the data is stored in a field named "data" in the stream entry.
        /// </summary>
        /// <param name="entry">The stream entry from which to deserialize data</param>
        /// <returns>The deserialized object of type <typeparamref name="T"/>.</returns>
        public T? GetDataFromEntry(StreamEntry entry)
        {
            var data = entry.Values.FirstOrDefault(x => x.Name == "data").Value;
            return JsonConvert.DeserializeObject<T>(data.ToString());
        }

        /// <summary>
        /// Acknowledges that a message has been processed and can be removed from the stream. This method should be called after successfully processing a message to prevent it from being re-delivered to other consumers.
        /// </summary>
        /// <param name="entry">The stream entry to acknowledge</param>
        /// <returns></returns>
        public async Task AcknowledgeEntry(StreamEntry entry)
        {
            await _database.StreamAcknowledgeAsync(name, groupName, entry.Id);
        }

        /// <summary>
        /// Reclaims crashed messages from the stream. This method will claim messages that have been idle for at least <paramref name="minIdleTimeMs"/> milliseconds indicating that the consumer that was processing them has crashed or is no longer active. The claimed messages will be reassigned to the current consumer.
        /// </summary>
        /// <param name="minIdleTimeMs">The minimum idle time in millseconds for messages to be reclaimed</param>
        /// <param name="count">The maximum number of messages to reclaim (default is 100)</param>
        /// <returns></returns>
        public Task<StreamAutoClaimResult> ReclaimCrashed(long minIdleTimeMs = 0, int? count = null)
        {
            return _database.StreamAutoClaimAsync(name, groupName, machineId, minIdleTimeMs, "0-0", count);
        }
    }
}
#pragma warning restore CS0618