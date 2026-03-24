using Lofka.Server.Protocol.Headers;
using Lofka.Server.Protocol.Primitives;
using Lofka.Server.Storage;

namespace Lofka.Server.Protocol.Messages.Admin;

public static class ListGroupsHandler
{
    public static void Handle(RequestHeader header, ReadOnlySpan<byte> body,
        BigEndianWriter writer, ConsumerGroupManager groupManager)
    {
        // Write response
        ResponseHeader.Write(writer, header.CorrelationId, header.ApiKey, header.ApiVersion);

        bool isFlexible = header.ApiVersion >= 4;

        // throttle_time_ms (v1+)
        if (header.ApiVersion >= 1)
            writer.WriteInt32(0);

        writer.WriteInt16(0); // error_code

        // groups array - return empty for now (groups are transient in our emulator)
        if (isFlexible)
        {
            writer.WriteCompactArrayLength(0);
            writer.WriteEmptyTagBuffer();
        }
        else
        {
            writer.WriteArrayLength(0);
        }
    }
}
