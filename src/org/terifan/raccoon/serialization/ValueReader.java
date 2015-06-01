package org.terifan.raccoon.serialization;

import java.io.IOException;
import java.util.Date;
import org.terifan.raccoon.util.ByteArrayBuffer;


class ValueReader
{
	static Object readValue(FieldType aFieldType, ByteArrayBuffer aDataInput) throws IOException
	{
		if (aFieldType.nullable && aDataInput.readBit() == 1)
		{
			return null;
		}

		Class type = aFieldType.type;

		if (type == Boolean.class || type == Boolean.TYPE)
		{
			return aDataInput.readBit() == 1;
		}
		if (type == Byte.class || type == Byte.TYPE)
		{
			return (byte)aDataInput.read();
		}
		if (type == Short.class || type == Short.TYPE)
		{
			return (short)aDataInput.readVar32();
		}
		if (type == Character.class || type == Character.TYPE)
		{
			return (char)aDataInput.readVar32();
		}
		if (type == Integer.class || type == Integer.TYPE)
		{
			return aDataInput.readVar32();
		}
		if (type == Long.class || type == Long.TYPE)
		{
			return aDataInput.readVar64();
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
			return aDataInput.readString(aDataInput.readVar32());
		}
		if (type == Date.class)
		{
			return new Date(aDataInput.readVar64());
		}

		throw new IllegalArgumentException("Unsupported type: " + type);
	}
}
