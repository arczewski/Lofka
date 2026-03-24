using Xunit;
using Lofka.Server.Protocol.Primitives;

namespace Lofka.Tests.Unit.Primitives;

public class BigEndianTests
{
    [Fact]
    public void ReadWrite_Int16_Roundtrip()
    {
        var writer = new BigEndianWriter();
        writer.WriteInt16(12345);
        writer.WriteInt16(-1);
        writer.WriteInt16(0);

        var reader = new BigEndianReader(writer.WrittenSpan);
        Assert.Equal(12345, reader.ReadInt16());
        Assert.Equal(-1, reader.ReadInt16());
        Assert.Equal(0, reader.ReadInt16());
    }

    [Fact]
    public void ReadWrite_Int32_Roundtrip()
    {
        var writer = new BigEndianWriter();
        writer.WriteInt32(int.MaxValue);
        writer.WriteInt32(int.MinValue);
        writer.WriteInt32(42);

        var reader = new BigEndianReader(writer.WrittenSpan);
        Assert.Equal(int.MaxValue, reader.ReadInt32());
        Assert.Equal(int.MinValue, reader.ReadInt32());
        Assert.Equal(42, reader.ReadInt32());
    }

    [Fact]
    public void ReadWrite_Int64_Roundtrip()
    {
        var writer = new BigEndianWriter();
        writer.WriteInt64(long.MaxValue);
        writer.WriteInt64(-1L);

        var reader = new BigEndianReader(writer.WrittenSpan);
        Assert.Equal(long.MaxValue, reader.ReadInt64());
        Assert.Equal(-1L, reader.ReadInt64());
    }

    [Fact]
    public void ReadWrite_NullableString_Roundtrip()
    {
        var writer = new BigEndianWriter();
        writer.WriteNullableString("hello");
        writer.WriteNullableString(null);
        writer.WriteNullableString("");

        var reader = new BigEndianReader(writer.WrittenSpan);
        Assert.Equal("hello", reader.ReadNullableString());
        Assert.Null(reader.ReadNullableString());
        Assert.Equal("", reader.ReadNullableString());
    }

    [Fact]
    public void ReadWrite_CompactString_Roundtrip()
    {
        var writer = new BigEndianWriter();
        writer.WriteCompactString("test");
        writer.WriteCompactNullableString(null);
        writer.WriteCompactString("");

        var reader = new BigEndianReader(writer.WrittenSpan);
        Assert.Equal("test", reader.ReadCompactString());
        Assert.Null(reader.ReadCompactNullableString());
        Assert.Equal("", reader.ReadCompactString());
    }

    [Fact]
    public void ReadWrite_CompactArrayLength_Roundtrip()
    {
        var writer = new BigEndianWriter();
        writer.WriteCompactArrayLength(5);
        writer.WriteCompactArrayLength(0);

        var reader = new BigEndianReader(writer.WrittenSpan);
        Assert.Equal(5, reader.ReadCompactArrayLength());
        Assert.Equal(0, reader.ReadCompactArrayLength());
    }

    [Fact]
    public void ToFramedBytes_IncludesSizePrefix()
    {
        var writer = new BigEndianWriter();
        writer.WriteInt32(42);

        var framed = writer.ToFramedBytes();
        Assert.Equal(8, framed.Length); // 4 bytes size + 4 bytes payload

        var reader = new BigEndianReader(framed);
        int size = reader.ReadInt32();
        Assert.Equal(4, size);
        Assert.Equal(42, reader.ReadInt32());
    }
}
