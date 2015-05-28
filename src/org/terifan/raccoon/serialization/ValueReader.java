package org.terifan.raccoon.serialization;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Date;


class ValueReader
{
	static Object readValue(Class type, DataInputStream aInputStream) throws IOException
	{
		if (type == Boolean.class || type == Boolean.TYPE)
		{
			return aInputStream.readBoolean();
		}
		if (type == Byte.class || type == Byte.TYPE)
		{
			return aInputStream.readByte();
		}
		if (type == Short.class || type == Short.TYPE)
		{
			return aInputStream.readShort();
		}
		if (type == Character.class || type == Character.TYPE)
		{
			return aInputStream.readChar();
		}
		if (type == Integer.class || type == Integer.TYPE)
		{
			return aInputStream.readInt();
		}
		if (type == Long.class || type == Long.TYPE)
		{
			return aInputStream.readLong();
		}
		if (type == Float.class || type == Float.TYPE)
		{
			return aInputStream.readFloat();
		}
		if (type == Double.class || type == Double.TYPE)
		{
			return aInputStream.readDouble();
		}
		if (type == String.class)
		{
			return aInputStream.readUTF();
		}
		if (type == Date.class)
		{
			return new Date(aInputStream.readLong());
		}

		throw new IllegalArgumentException("Unsupported type: " + type);
	}
}
