using System.Buffers.Binary;
using System.Net.Sockets;
using Lofka.Server.Protocol;
using Lofka.Server.Protocol.Headers;

namespace Lofka.Server.Network;

public sealed class ClientConnection
{
    private readonly TcpClient _client;
    private readonly RequestDispatcher _dispatcher;

    public ClientConnection(TcpClient client, RequestDispatcher dispatcher)
    {
        _client = client;
        _dispatcher = dispatcher;
    }

    public async Task RunAsync(CancellationToken ct)
    {
        try
        {
            var stream = _client.GetStream();
            var sizeBuffer = new byte[4];

            while (!ct.IsCancellationRequested && _client.Connected)
            {
                // Read 4-byte frame size
                int bytesRead = await ReadExactlyAsync(stream, sizeBuffer, 0, 4, ct);
                if (bytesRead < 4) break; // Client disconnected

                int frameSize = BinaryPrimitives.ReadInt32BigEndian(sizeBuffer);
                if (frameSize <= 0 || frameSize > 100 * 1024 * 1024) break; // Sanity check

                // Read frame payload
                var payload = new byte[frameSize];
                bytesRead = await ReadExactlyAsync(stream, payload, 0, frameSize, ct);
                if (bytesRead < frameSize) break;

                // Parse header
                var (header, bodyOffset) = RequestHeader.Parse(payload);

                // Dispatch and get response
                var response = await _dispatcher.DispatchAsync(header, payload.AsMemory(bodyOffset), ct);

                // Write framed response
                await stream.WriteAsync(response, ct);
            }
        }
        catch (IOException)
        {
            // Client disconnected
        }
        catch (OperationCanceledException)
        {
            // Server shutting down
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"Connection error: {ex}");
        }
        finally
        {
            _client.Dispose();
        }
    }

    private static async Task<int> ReadExactlyAsync(NetworkStream stream, byte[] buffer, int offset, int count, CancellationToken ct)
    {
        int totalRead = 0;
        while (totalRead < count)
        {
            int read = await stream.ReadAsync(buffer.AsMemory(offset + totalRead, count - totalRead), ct);
            if (read == 0) return totalRead; // EOF
            totalRead += read;
        }
        return totalRead;
    }
}
