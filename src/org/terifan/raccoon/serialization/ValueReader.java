package org.terifan.raccoon.serialization;

import java.io.DataInput;
import java.io.IOException;
import java.util.Date;


class ValueReader
{
	static Object readValue(FieldType aFieldType, DataInput aDataInput) throws IOException
	{
		if (!aFieldType.primitive && aDataInput.readBoolean())
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
			return aDataInput.readShort();
		}
		if (type == Character.class || type == Character.TYPE)
		{
			return aDataInput.readChar();
		}
		if (type == Integer.class || type == Integer.TYPE)
		{
			return aDataInput.readInt();
		}
		if (type == Long.class || type == Long.TYPE)
		{
			return aDataInput.readLong();
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
			return aDataInput.readUTF();
		}
		if (type == Date.class)
		{
			return new Date(aDataInput.readLong());
		}

		throw new IllegalArgumentException("Unsupported type: " + type);
	}
}
