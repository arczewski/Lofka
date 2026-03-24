namespace Lofka.Server.Protocol.Primitives;

/// <summary>
/// Unsigned varint and zigzag signed varint encoding/decoding.
/// </summary>
public static class VarIntEncoder
{
    public static uint ReadUnsignedVarint(ReadOnlySpan<byte> buffer, ref int offset)
    {
        uint result = 0;
        int shift = 0;
        while (true)
        {
            byte b = buffer[offset++];
            result |= (uint)(b & 0x7F) << shift;
            if ((b & 0x80) == 0) break;
            shift += 7;
            if (shift > 28)
                throw new InvalidOperationException("Varint too long");
        }
        return result;
    }

    public static int WriteUnsignedVarint(Span<byte> buffer, uint value)
    {
        int written = 0;
        while ((value & ~0x7Fu) != 0)
        {
            buffer[written++] = (byte)((value & 0x7F) | 0x80);
            value >>= 7;
        }
        buffer[written++] = (byte)value;
        return written;
    }

    public static int ReadSignedVarint(ReadOnlySpan<byte> buffer, ref int offset)
    {
        uint raw = ReadUnsignedVarint(buffer, ref offset);
        return (int)((raw >> 1) ^ -(raw & 1));
    }

    public static int WriteSignedVarint(Span<byte> buffer, int value)
    {
        uint zigzag = (uint)((value << 1) ^ (value >> 31));
        return WriteUnsignedVarint(buffer, zigzag);
    }

    public static ulong ReadUnsignedVarlong(ReadOnlySpan<byte> buffer, ref int offset)
    {
        ulong result = 0;
        int shift = 0;
        while (true)
        {
            byte b = buffer[offset++];
            result |= (ulong)(b & 0x7F) << shift;
            if ((b & 0x80) == 0) break;
            shift += 7;
            if (shift > 63)
                throw new InvalidOperationException("Varlong too long");
        }
        return result;
    }

    public static long ReadSignedVarlong(ReadOnlySpan<byte> buffer, ref int offset)
    {
        ulong raw = ReadUnsignedVarlong(buffer, ref offset);
        return (long)((raw >> 1) ^ (ulong)(-(long)(raw & 1)));
    }
}
