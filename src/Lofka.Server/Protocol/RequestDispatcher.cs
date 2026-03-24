using Lofka.Server.Configuration;
using Lofka.Server.Network;
using Lofka.Server.Protocol.Headers;
using Lofka.Server.Protocol.Messages.Admin;
using Lofka.Server.Protocol.Messages.ApiVersions;
using Lofka.Server.Protocol.Messages.ConsumerGroup;
using Lofka.Server.Protocol.Messages.Fetch;
using Lofka.Server.Protocol.Messages.FindCoordinator;
using Lofka.Server.Protocol.Messages.ListOffsets;
using Lofka.Server.Protocol.Messages.Metadata;
using Lofka.Server.Protocol.Messages.Offsets;
using Lofka.Server.Protocol.Messages.Produce;
using Lofka.Server.Protocol.Primitives;
using Lofka.Server.Storage;

namespace Lofka.Server.Protocol;

public sealed class RequestDispatcher
{
    private readonly ServerConfig _config;
    private readonly TopicStore _topicStore;
    private readonly ConsumerGroupManager _groupManager;
    private readonly OffsetStore _offsetStore;
    private readonly LofkaServer _server;

    public RequestDispatcher(ServerConfig config, TopicStore topicStore,
        ConsumerGroupManager groupManager, OffsetStore offsetStore, LofkaServer server)
    {
        _config = config;
        _topicStore = topicStore;
        _groupManager = groupManager;
        _offsetStore = offsetStore;
        _server = server;
    }

    public async Task<byte[]> DispatchAsync(RequestHeader header, ReadOnlyMemory<byte> body, CancellationToken ct)
    {
        LofkaLogger.Request(header.ApiKey, header.ApiVersion, header.CorrelationId, header.ClientId);

        var writer = new BigEndianWriter(512);

        switch (header.ApiKey)
        {
            case 0:
                ProduceHandler.Handle(header, body.Span, writer, _topicStore, _config);
                break;
            case 1:
                await FetchHandler.HandleAsync(header, body, writer, _topicStore, ct);
                break;
            case 2:
                ListOffsetsHandler.Handle(header, body.Span, writer, _topicStore);
                break;
            case 3:
                MetadataHandler.Handle(header, body.Span, writer, _topicStore, _config);
                break;
            case 8:
                OffsetCommitHandler.Handle(header, body.Span, writer, _offsetStore);
                break;
            case 9:
                OffsetFetchHandler.Handle(header, body.Span, writer, _offsetStore);
                break;
            case 10:
                FindCoordinatorHandler.Handle(header, body.Span, writer, _config);
                break;
            case 11:
                await JoinGroupHandler.HandleAsync(header, body, writer, _groupManager, ct);
                break;
            case 12:
                HeartbeatHandler.Handle(header, body.Span, writer, _groupManager);
                break;
            case 13:
                LeaveGroupHandler.Handle(header, body.Span, writer, _groupManager);
                break;
            case 14:
                await SyncGroupHandler.HandleAsync(header, body, writer, _groupManager, ct);
                break;
            case 18:
                ApiVersionsHandler.Handle(header, writer);
                break;
            case 19:
                CreateTopicsHandler.Handle(header, body.Span, writer, _topicStore);
                break;
            case 20:
                DeleteTopicsHandler.Handle(header, body.Span, writer, _topicStore);
                break;
            case 15:
                DescribeGroupsHandler.Handle(header, body.Span, writer, _groupManager);
                break;
            case 16:
                ListGroupsHandler.Handle(header, body.Span, writer, _groupManager);
                break;
            case 22:
                InitProducerIdHandler.Handle(header, body.Span, writer, _server);
                break;
            case 32:
                DescribeConfigsHandler.Handle(header, body.Span, writer);
                break;
            case 60:
                DescribeClusterHandler.Handle(header, body.Span, writer, _config);
                break;
            default:
                LofkaLogger.Warn($"Unsupported API key {header.ApiKey} ({LofkaLogger.GetApiName(header.ApiKey)}) v{header.ApiVersion}");
                WriteUnsupportedApiResponse(header, writer);
                break;
        }

        var response = writer.ToFramedBytes();
        LofkaLogger.Response(header.ApiKey, header.CorrelationId, response.Length);
        return response;
    }

    private static void WriteUnsupportedApiResponse(RequestHeader header, BigEndianWriter writer)
    {
        ResponseHeader.Write(writer, header.CorrelationId, header.ApiKey, header.ApiVersion);
        writer.WriteInt16(35); // UNSUPPORTED_VERSION
    }
}
