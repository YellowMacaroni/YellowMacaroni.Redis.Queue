using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Text;

namespace YellowMacaroni.Redis.Queue
{
    public static class Extensions
    {
        public static string GetId(this StreamEntry entry)
        {
            return entry.Values.FirstOrDefault(x => x.Name == "id").Value.ToString();
        }
        public static string GetShortId(this StreamEntry entry)
        {
            return entry.GetId().Split('-')[0];
        }

        public static string GetData(this StreamEntry entry)
        {
            return entry.Values.FirstOrDefault(x => x.Name == "data").Value.ToString();
        }

        public static int GetAttempt(this StreamEntry entry)
        {
            return int.Parse(entry.Values.FirstOrDefault(x => x.Name == "attempt").Value.ToString());
        }

        public static long GetUnixTimestampMs(this StreamEntry entry)
        {
            return long.Parse(entry.Values.FirstOrDefault(x => x.Name == "timestamp").Value.ToString());
        }

        public static DateTimeOffset GetTimestamp(this StreamEntry entry)
        {
            return DateTimeOffset.FromUnixTimeMilliseconds(GetUnixTimestampMs(entry));
        }

        public static async Task<TimeSpan> GetDurationSinceEnqueueAsync<T>(this StreamEntry entry, RedisQueue<T> queue)
        {
            var timestamp = GetTimestamp(entry);
            var now = await queue.GetServerTimeOffsetAsync();
            return now - timestamp;
        }

        public static async Task<long> GetMillisecondsSinceEnqueueAsync<T>(this StreamEntry entry, RedisQueue<T> queue)
        {
            var duration = await GetDurationSinceEnqueueAsync(entry, queue);
            return (long)duration.TotalMilliseconds;
        }
    }
}
