using Lofka.Server.Network;
using Lofka.Server.Protocol.Headers;
using Lofka.Server.Protocol.Primitives;

namespace Lofka.Server.Protocol.Messages.Admin;

public static class InitProducerIdHandler
{
    public static void Handle(RequestHeader header, ReadOnlySpan<byte> body,
        BigEndianWriter writer, LofkaServer server)
    {
        var reader = new BigEndianReader(body);
        bool isFlexible = header.ApiVersion >= 2;

        // transactional_id (nullable)
        if (isFlexible) reader.ReadCompactNullableString();
        else reader.ReadNullableString();

        // transaction_timeout_ms
        reader.ReadInt32();

        // producer_id (v3+)
        if (header.ApiVersion >= 3)
            reader.ReadInt64();

        // producer_epoch (v3+)
        if (header.ApiVersion >= 3)
            reader.ReadInt16();

        long producerId = server.GetNextProducerId();

        // Write response
        ResponseHeader.Write(writer, header.CorrelationId, header.ApiKey, header.ApiVersion);

        // throttle_time_ms
        writer.WriteInt32(0);
        writer.WriteInt16(0);     // error_code
        writer.WriteInt64(producerId);
        writer.WriteInt16(0);     // producer_epoch

        if (isFlexible)
            writer.WriteEmptyTagBuffer();
    }
}
