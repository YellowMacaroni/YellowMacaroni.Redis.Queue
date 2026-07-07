using System;
using System.Collections.Generic;
using System.Text;

namespace YellowMacaroni.Redis.Queue
{
    public class QueueResponse<R> where R: class
    {
        public R? data;

        public bool shouldRetry = false;
        public long retryInMs = -1;

        public static QueueResponse<R> Success(R data)
        {
            return new QueueResponse<R>
            {
                data = data
            };
        }

        public static QueueResponse<R> Retry(R? data = null, long retryInMs = -1)
        {
            return new QueueResponse<R>
            {
                data = data,
                shouldRetry = true,
                retryInMs = retryInMs
            };
        }

        public static QueueResponse<R> Retry(long retryInMs = -1)
        {
            return new QueueResponse<R>
            {
                shouldRetry = true,
                retryInMs = retryInMs
            };
        }

        public static QueueResponse<R> Retry()
        {
            return new QueueResponse<R>
            {
                shouldRetry = true
            };
        }
    }
}
