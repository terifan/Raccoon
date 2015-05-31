package org.terifan.raccoon.serialization;

import java.io.DataInput;
import java.io.IOException;
import java.util.Date;
import org.terifan.raccoon.util.ByteArray;


class ValueReader
{
	static Object readValue(FieldType aFieldType, DataInput aDataInput) throws IOException
	{
		if (aFieldType.nullable && aDataInput.readBoolean())
		{
			return null;
		}

		Class type = aFieldType.type;

		if (type == Boolean.class || type == Boolean.TYPE)
		{
			return aDataInput.readBoolean();
		}
		if (type == Byte.class || type == Byte.TYPE)
		{
			return aDataInput.readByte();
		}
		if (type == Short.class || type == Short.TYPE)
		{
			return (short)ByteArray.readVarInt(aDataInput);
		}
		if (type == Character.class || type == Character.TYPE)
		{
			return (char)ByteArray.readVarInt(aDataInput);
		}
		if (type == Integer.class || type == Integer.TYPE)
		{
			return ByteArray.readVarInt(aDataInput);
		}
		if (type == Long.class || type == Long.TYPE)
		{
			return ByteArray.readVarLong(aDataInput);
		}
		if (type == Float.class || type == Float.TYPE)
		{
			return aDataInput.readFloat();
		}
		if (type == Double.class || type == Double.TYPE)
		{
			return aDataInput.readDouble();
		}
		if (type == String.class)
		{
			return ByteArray.decodeUTF8(aDataInput, ByteArray.readVarInt(aDataInput));
		}
		if (type == Date.class)
		{
			return new Date(ByteArray.readVarLong(aDataInput));
		}

		throw new IllegalArgumentException("Unsupported type: " + type);
	}
}
