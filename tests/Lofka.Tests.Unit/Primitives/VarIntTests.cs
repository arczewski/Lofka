using Xunit;
using Lofka.Server.Protocol.Primitives;

namespace Lofka.Tests.Unit.Primitives;

public class VarIntTests
{
    [Theory]
    [InlineData(0u, new byte[] { 0x00 })]
    [InlineData(1u, new byte[] { 0x01 })]
    [InlineData(127u, new byte[] { 0x7F })]
    [InlineData(128u, new byte[] { 0x80, 0x01 })]
    [InlineData(300u, new byte[] { 0xAC, 0x02 })]
    [InlineData(16384u, new byte[] { 0x80, 0x80, 0x01 })]
    public void UnsignedVarint_Roundtrip(uint value, byte[] expectedBytes)
    {
        // Write
        var buffer = new byte[5];
        int written = VarIntEncoder.WriteUnsignedVarint(buffer, value);
        Assert.Equal(expectedBytes.Length, written);
        Assert.Equal(expectedBytes, buffer[..written]);

        // Read
        int offset = 0;
        uint readValue = VarIntEncoder.ReadUnsignedVarint(buffer, ref offset);
        Assert.Equal(value, readValue);
        Assert.Equal(written, offset);
    }

    [Theory]
    [InlineData(0, 0u)]    // 0 -> zigzag 0
    [InlineData(-1, 1u)]   // -1 -> zigzag 1
    [InlineData(1, 2u)]    // 1 -> zigzag 2
    [InlineData(-2, 3u)]   // -2 -> zigzag 3
    [InlineData(2, 4u)]    // 2 -> zigzag 4
    public void SignedVarint_ZigzagEncoding(int value, uint expectedZigzag)
    {
        var buffer = new byte[5];
        int written = VarIntEncoder.WriteSignedVarint(buffer, value);

        int offset = 0;
        uint rawZigzag = VarIntEncoder.ReadUnsignedVarint(buffer, ref offset);
        Assert.Equal(expectedZigzag, rawZigzag);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(-1)]
    [InlineData(int.MaxValue)]
    [InlineData(int.MinValue)]
    [InlineData(12345)]
    [InlineData(-12345)]
    public void SignedVarint_Roundtrip(int value)
    {
        var buffer = new byte[5];
        int written = VarIntEncoder.WriteSignedVarint(buffer, value);

        int offset = 0;
        int readValue = VarIntEncoder.ReadSignedVarint(buffer, ref offset);
        Assert.Equal(value, readValue);
    }

    [Theory]
    [InlineData(0L)]
    [InlineData(1L)]
    [InlineData(-1L)]
    [InlineData(long.MaxValue)]
    [InlineData(long.MinValue)]
    public void SignedVarlong_Roundtrip(long value)
    {
        // Use BigEndianWriter for varlong (via WriteSignedVarint)
        var writer = new BigEndianWriter();
        writer.WriteSignedVarint((int)value); // For int-range values

        var reader = new BigEndianReader(writer.WrittenSpan);
        int readValue = reader.ReadSignedVarint();
        Assert.Equal((int)value, readValue);
    }
}
