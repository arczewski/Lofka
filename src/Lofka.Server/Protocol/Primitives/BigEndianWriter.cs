using System.Buffers.Binary;
using System.Text;

namespace Lofka.Server.Protocol.Primitives;

/// <summary>
/// Writes big-endian values to a growable byte buffer.
/// </summary>
public sealed class BigEndianWriter
{
    private byte[] _buffer;
    private int _offset;

    public BigEndianWriter(int initialCapacity = 256)
    {
        _buffer = new byte[initialCapacity];
        _offset = 0;
    }

    public int Length => _offset;

    public ReadOnlySpan<byte> WrittenSpan => _buffer.AsSpan(0, _offset);
    public byte[] Buffer => _buffer;

    private void EnsureCapacity(int additionalBytes)
    {
        int required = _offset + additionalBytes;
        if (required <= _buffer.Length) return;
        int newSize = Math.Max(_buffer.Length * 2, required);
        Array.Resize(ref _buffer, newSize);
    }

    public void WriteInt8(byte value)
    {
        EnsureCapacity(1);
        _buffer[_offset++] = value;
    }

    public void WriteInt16(short value)
    {
        EnsureCapacity(2);
        BinaryPrimitives.WriteInt16BigEndian(_buffer.AsSpan(_offset), value);
        _offset += 2;
    }

    public void WriteInt32(int value)
    {
        EnsureCapacity(4);
        BinaryPrimitives.WriteInt32BigEndian(_buffer.AsSpan(_offset), value);
        _offset += 4;
    }

    public void WriteInt64(long value)
    {
        EnsureCapacity(8);
        BinaryPrimitives.WriteInt64BigEndian(_buffer.AsSpan(_offset), value);
        _offset += 8;
    }

    public void WriteBool(bool value)
    {
        WriteInt8(value ? (byte)1 : (byte)0);
    }

    /// <summary>Writes a non-compact nullable string (int16 length prefix).</summary>
    public void WriteNullableString(string? value)
    {
        if (value == null)
        {
            WriteInt16(-1);
            return;
        }
        var bytes = Encoding.UTF8.GetBytes(value);
        WriteInt16((short)bytes.Length);
        WriteRawBytes(bytes);
    }

    /// <summary>Writes a non-compact string.</summary>
    public void WriteString(string value) => WriteNullableString(value);

    /// <summary>Writes non-compact nullable bytes (int32 length prefix).</summary>
    public void WriteNullableBytes(byte[]? value)
    {
        if (value == null)
        {
            WriteInt32(-1);
            return;
        }
        WriteInt32(value.Length);
        WriteRawBytes(value);
    }

    /// <summary>Writes a non-compact array count (int32).</summary>
    public void WriteArrayLength(int count) => WriteInt32(count);

    /// <summary>Writes an unsigned varint.</summary>
    public void WriteUnsignedVarint(uint value)
    {
        EnsureCapacity(5);
        _offset += VarIntEncoder.WriteUnsignedVarint(_buffer.AsSpan(_offset), value);
    }

    /// <summary>Writes a signed varint (zigzag).</summary>
    public void WriteSignedVarint(int value)
    {
        uint zigzag = (uint)((value << 1) ^ (value >> 31));
        WriteUnsignedVarint(zigzag);
    }

    /// <summary>Writes a compact nullable string (uvarint length+1 prefix).</summary>
    public void WriteCompactNullableString(string? value)
    {
        if (value == null)
        {
            WriteUnsignedVarint(0);
            return;
        }
        var bytes = Encoding.UTF8.GetBytes(value);
        WriteUnsignedVarint((uint)(bytes.Length + 1));
        WriteRawBytes(bytes);
    }

    /// <summary>Writes a compact string (non-nullable).</summary>
    public void WriteCompactString(string value) => WriteCompactNullableString(value);

    /// <summary>Writes a compact array count (uvarint count+1).</summary>
    public void WriteCompactArrayLength(int count)
    {
        WriteUnsignedVarint((uint)(count + 1));
    }

    /// <summary>Writes compact nullable bytes (uvarint length+1 prefix).</summary>
    public void WriteCompactNullableBytes(byte[]? value)
    {
        if (value == null)
        {
            WriteUnsignedVarint(0);
            return;
        }
        WriteUnsignedVarint((uint)(value.Length + 1));
        WriteRawBytes(value);
    }

    /// <summary>Writes an empty tag buffer (zero tagged fields).</summary>
    public void WriteEmptyTagBuffer()
    {
        WriteUnsignedVarint(0);
    }

    public void WriteRawBytes(ReadOnlySpan<byte> data)
    {
        EnsureCapacity(data.Length);
        data.CopyTo(_buffer.AsSpan(_offset));
        _offset += data.Length;
    }

    /// <summary>Returns the current offset for later patching.</summary>
    public int ReserveInt32()
    {
        int pos = _offset;
        WriteInt32(0);
        return pos;
    }

    /// <summary>Patches a previously reserved int32 at the given position.</summary>
    public void PatchInt32(int position, int value)
    {
        BinaryPrimitives.WriteInt32BigEndian(_buffer.AsSpan(position), value);
    }

    /// <summary>Writes the complete response frame: [int32 size][payload].</summary>
    public byte[] ToFramedBytes()
    {
        var result = new byte[4 + _offset];
        BinaryPrimitives.WriteInt32BigEndian(result, _offset);
        _buffer.AsSpan(0, _offset).CopyTo(result.AsSpan(4));
        return result;
    }

    public void Reset()
    {
        _offset = 0;
    }
}
