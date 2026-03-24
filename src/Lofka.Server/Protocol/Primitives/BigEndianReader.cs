using System.Buffers.Binary;
using System.Text;

namespace Lofka.Server.Protocol.Primitives;

/// <summary>
/// Reads big-endian values from a byte span, advancing an internal offset.
/// </summary>
public ref struct BigEndianReader
{
    private readonly ReadOnlySpan<byte> _buffer;
    private int _offset;

    public BigEndianReader(ReadOnlySpan<byte> buffer)
    {
        _buffer = buffer;
        _offset = 0;
    }

    public int Offset => _offset;
    public int Remaining => _buffer.Length - _offset;

    public byte ReadInt8()
    {
        return _buffer[_offset++];
    }

    public short ReadInt16()
    {
        var value = BinaryPrimitives.ReadInt16BigEndian(_buffer.Slice(_offset));
        _offset += 2;
        return value;
    }

    public int ReadInt32()
    {
        var value = BinaryPrimitives.ReadInt32BigEndian(_buffer.Slice(_offset));
        _offset += 4;
        return value;
    }

    public long ReadInt64()
    {
        var value = BinaryPrimitives.ReadInt64BigEndian(_buffer.Slice(_offset));
        _offset += 8;
        return value;
    }

    public bool ReadBool()
    {
        return ReadInt8() != 0;
    }

    /// <summary>Reads a non-compact nullable string (int16 length prefix).</summary>
    public string? ReadNullableString()
    {
        short length = ReadInt16();
        if (length < 0) return null;
        var value = Encoding.UTF8.GetString(_buffer.Slice(_offset, length));
        _offset += length;
        return value;
    }

    /// <summary>Reads a non-compact string (int16 length prefix, non-nullable).</summary>
    public string ReadString()
    {
        return ReadNullableString() ?? string.Empty;
    }

    /// <summary>Reads non-compact nullable bytes (int32 length prefix).</summary>
    public byte[]? ReadNullableBytes()
    {
        int length = ReadInt32();
        if (length < 0) return null;
        var value = _buffer.Slice(_offset, length).ToArray();
        _offset += length;
        return value;
    }

    /// <summary>Reads non-compact bytes (int32 length prefix).</summary>
    public byte[] ReadBytes()
    {
        return ReadNullableBytes() ?? Array.Empty<byte>();
    }

    /// <summary>Reads the count for a non-compact array (int32). Returns -1 for null.</summary>
    public int ReadArrayLength()
    {
        return ReadInt32();
    }

    /// <summary>Reads an unsigned varint.</summary>
    public uint ReadUnsignedVarint()
    {
        return VarIntEncoder.ReadUnsignedVarint(_buffer, ref _offset);
    }

    /// <summary>Reads a signed varint (zigzag encoded).</summary>
    public int ReadSignedVarint()
    {
        return VarIntEncoder.ReadSignedVarint(_buffer, ref _offset);
    }

    /// <summary>Reads a signed varlong (zigzag encoded).</summary>
    public long ReadSignedVarlong()
    {
        return VarIntEncoder.ReadSignedVarlong(_buffer, ref _offset);
    }

    /// <summary>Reads a compact nullable string (uvarint length+1 prefix).</summary>
    public string? ReadCompactNullableString()
    {
        uint lengthPlusOne = ReadUnsignedVarint();
        if (lengthPlusOne == 0) return null;
        int length = (int)(lengthPlusOne - 1);
        var value = Encoding.UTF8.GetString(_buffer.Slice(_offset, length));
        _offset += length;
        return value;
    }

    /// <summary>Reads a compact string (non-nullable).</summary>
    public string ReadCompactString()
    {
        return ReadCompactNullableString() ?? string.Empty;
    }

    /// <summary>Reads the count for a compact array (uvarint count+1). Returns -1 for null.</summary>
    public int ReadCompactArrayLength()
    {
        uint countPlusOne = ReadUnsignedVarint();
        if (countPlusOne == 0) return -1;
        return (int)(countPlusOne - 1);
    }

    /// <summary>Reads compact nullable bytes (uvarint length+1 prefix).</summary>
    public byte[]? ReadCompactNullableBytes()
    {
        uint lengthPlusOne = ReadUnsignedVarint();
        if (lengthPlusOne == 0) return null;
        int length = (int)(lengthPlusOne - 1);
        var value = _buffer.Slice(_offset, length).ToArray();
        _offset += length;
        return value;
    }

    /// <summary>Reads compact bytes (non-nullable).</summary>
    public byte[] ReadCompactBytes()
    {
        return ReadCompactNullableBytes() ?? Array.Empty<byte>();
    }

    /// <summary>Skips the tag buffer (used in flexible versions).</summary>
    public void SkipTagBuffer()
    {
        uint numTags = ReadUnsignedVarint();
        for (uint i = 0; i < numTags; i++)
        {
            ReadUnsignedVarint(); // tag
            uint dataLen = ReadUnsignedVarint();
            _offset += (int)dataLen;
        }
    }

    /// <summary>Reads a raw span of the given length without copying.</summary>
    public ReadOnlySpan<byte> ReadRawBytes(int length)
    {
        var span = _buffer.Slice(_offset, length);
        _offset += length;
        return span;
    }

    public void Skip(int count)
    {
        _offset += count;
    }
}
