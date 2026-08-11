using System.Reflection;

namespace YellowMacaroni.Redis.Queue.Scripts
{
    internal static class Load
    {
        internal static readonly string StreamAdd = Read("StreamAdd.lua");
        internal static readonly string AddDelayed = Read("AddDelayed.lua");
        internal static readonly string PromoteDelayed = Read("PromoteDelayed.lua");

        private static string Read(string fileName)
        {
            var assembly = typeof(Load).Assembly;
            var resourceName = $"{typeof(Load).Namespace}.{fileName}";

            using var stream = assembly.GetManifestResourceStream(resourceName)
                ?? throw new InvalidOperationException(
                    $"Embedded Lua script '{resourceName}' was not found in '{assembly.GetName().Name}'.");
            using var reader = new StreamReader(stream);

            return reader.ReadToEnd();
        }
    }
}
