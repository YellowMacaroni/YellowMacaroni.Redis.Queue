## YellowMacaroni.Redis.Queue
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