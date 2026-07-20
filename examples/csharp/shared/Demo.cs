namespace KubeMQ.Kafka.Examples.Shared;

/// <summary>
/// Console-output and assertion helpers shared by every example program.
///
/// Examples are runnable proofs, not demos: each prints clear human-readable
/// progress and MUST exit non-zero on any failed assertion or unexpected error.
/// <see cref="Require"/> throws <see cref="DemoFailure"/> which the per-program
/// top-level try/catch turns into <c>Environment.Exit(1)</c>.
/// </summary>
public static class Demo
{
    /// <summary>Prints a progress step, e.g. <c>[*] Created queue 'orders'</c>.</summary>
    public static void Step(string message) => Console.WriteLine($"[*] {message}");

    /// <summary>Prints a send/produce action, e.g. <c>[x] Sent message id=...</c>.</summary>
    public static void Sent(string message) => Console.WriteLine($"[x] {message}");

    /// <summary>Prints a receive/observe action, e.g. <c>[v] Received '...'</c>.</summary>
    public static void Got(string message) => Console.WriteLine($"[v] {message}");

    /// <summary>Prints a final success banner.</summary>
    public static void Ok(string message) => Console.WriteLine($"[ok] {message}");

    /// <summary>Asserts a condition; throws <see cref="DemoFailure"/> (→ exit 1) when false.</summary>
    public static void Require(bool condition, string message)
    {
        if (!condition) throw new DemoFailure(message);
    }

    /// <summary>Asserts equality and reports both values on mismatch.</summary>
    public static void RequireEqual<T>(T expected, T actual, string what)
    {
        if (!EqualityComparer<T>.Default.Equals(expected, actual))
            throw new DemoFailure($"{what}: expected '{expected}', got '{actual}'");
    }

    /// <summary>
    /// Runs the example body, mapping any <see cref="DemoFailure"/> or unexpected
    /// exception to a non-zero process exit (the SHARED-CONVENTIONS exit-code rule).
    /// </summary>
    public static async Task<int> RunAsync(Func<Task> body)
    {
        try
        {
            await body();
            return 0;
        }
        catch (DemoFailure ex)
        {
            Console.Error.WriteLine($"[FAIL] {ex.Message}");
            return 1;
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"[ERROR] {ex.GetType().Name}: {ex.Message}");
            return 1;
        }
    }
}

/// <summary>Raised by <see cref="Demo.Require"/> on a failed assertion.</summary>
public sealed class DemoFailure(string message) : Exception(message);
