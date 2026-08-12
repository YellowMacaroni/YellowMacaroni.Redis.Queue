## YellowMacaroni.Redis.Queue
[![CI/CD](https://github.com/YellowMacaroni/YellowMacaroni.Redis.Queue/actions/workflows/ci.yml/badge.svg)](https://github.com/YellowMacaroni/YellowMacaroni.Redis.Queue/actions/workflows/ci.yml)
[![GitHub latest commit](https://badgen.net/github/last-commit/YellowMacaroni/YellowMacaroni.Redis.Queue)](https://GitHub.com/YellowMacaroni/YellowMacaroni.Redis.Queue/commit/)
[![NuGet stable version](https://badgen.net/nuget/v/yellowmacaroni.redis.queue)](https://nuget.org/packages/yellowmacaroni.redis.queue)
[![NuGet pre version](https://badgen.net/nuget/v/yellowmacaroni.redis.queue/pre)](https://nuget.org/packages/yellowmacaroni.redis.queue)
[![GitHub license](https://badgen.net/github/license/YellowMacaroni/YellowMacaroni.Redis.Queue)](https://github.com/YellowMacaroni/YellowMacaroni.Redis.Queue/blob/master/LICENSE)

A .NET package that provides a simple way to work with Redis queues.

### How to use
Base
```cs
var client = new QueueClient($"{host}:{port},password={password},abortConnect=false");

// Create a queue with the name "myqueue" and the type of data it will hold (in this case, object for anything)
var queue = new RedisQueue<object>(client, "myqueue");
```

Client
```cs
// This doesn't expect a response from the server
queue.Enqueue(new { message = "hello world!" });

// This expects a response from the server (the type param is the return type)
var response = await queue.Enqueue<object>(new { message = "hello world!" });
```

Server (With Response)
```cs
await queue.ListenForMessagesWithCallback(
    async (entry) =>
    {
        var message = queue.GetDataFromEntry(entry);
        if (message is null) return QueueResponse<string>.Retry();

        Console.WriteLine(message.message);

        return QueueResponse<string>.Success(new { message = "got your message!" });
    },
    cts.Token
);
```
