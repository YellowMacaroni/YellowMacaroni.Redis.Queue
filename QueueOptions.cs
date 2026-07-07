using System;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;

namespace YellowMacaroni.Redis.Queue
{
    public partial class QueueOptions()
    {
        public string? MachineId;
        public string? GroupName;
        public bool? PublishEvents;
        public int? MinEntryAgeMs;
        public int? PollWaitMs;
        public int? ReturnMaxWaitMs;
        public int? DequeueCount;
        public long? MaxStoreResponseDuration;
        public QueuePosition? QueuePositionToRetrieveFrom;
        public int MaxRetries = 3;
        public Backoff BackoffStrategy = Backoff.Exponential;
        public int BackoffBaseMs = 1000;

        public static QueueOptions FromEnvironment()
        {
            var options = new QueueOptions();

            var fields = typeof(QueueOptions).GetFields();

            foreach (var field in fields)
            {
                var envVar = $"QUEUE_{EnvironmentVariableRegex().Replace(field.Name, "$1_$2").ToUpper()}";
                var envValue = Environment.GetEnvironmentVariable(envVar);

                if (envValue is not null)
                {
                    field.SetValue(options, envValue);
                }
            }

            return options;
        }

        [GeneratedRegex(@"([a-z])([A-Z])")]
        private static partial Regex EnvironmentVariableRegex();

        public enum QueuePosition
        {
            Head = 0,
            NotClaimed = 1,
            Tail = 2
        }

        public enum Backoff
        {
            Linear = 0,
            Exponential = 1
        }

        public string GetQueuePositionString()
        {
            return QueuePositionToRetrieveFrom switch
            {
                QueuePosition.Head => "0",
                QueuePosition.NotClaimed => ">",
                QueuePosition.Tail => "<",
                _ => ">",
            };
        }

        public int GetBackoff(int attempt)
        {
            return BackoffStrategy switch
            {
                Backoff.Linear => BackoffBaseMs * attempt,
                Backoff.Exponential => (int)(BackoffBaseMs * Math.Pow(2, attempt - 1)),
                _ => BackoffBaseMs * attempt,
            };
        }
    }
}
