package org.terifan.raccoon.serialization;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.Date;
import org.terifan.raccoon.util.ByteArrayBuffer;


class ValueReader
{
	static Object readValue(Class aType, ByteArrayBuffer aDataInput) throws IOException
	{
		if (aType == Boolean.class || aType == Boolean.TYPE)
		{
			return aDataInput.readBit() == 1;
		}
		if (aType == Byte.class || aType == Byte.TYPE)
		{
			return (byte)aDataInput.read();
		}
		if (aType == Short.class || aType == Short.TYPE)
		{
			return (short)aDataInput.readVar32();
		}
		if (aType == Character.class || aType == Character.TYPE)
		{
			return (char)aDataInput.readVar32();
		}
		if (aType == Integer.class || aType == Integer.TYPE)
		{
			return aDataInput.readVar32();
		}
		if (aType == Long.class || aType == Long.TYPE)
		{
			return aDataInput.readVar64();
		}
		if (aType == Float.class || aType == Float.TYPE)
		{
			return aDataInput.readFloat();
		}
		if (aType == Double.class || aType == Double.TYPE)
		{
			return aDataInput.readDouble();
		}
		if (aType == String.class)
		{
			return aDataInput.readString(aDataInput.readVar32());
		}
		if (aType == Date.class)
		{
			return new Date(aDataInput.readVar64());
		}
		if (Serializable.class.isAssignableFrom(aType))
		{
			byte[] buffer = new byte[aDataInput.readVar32()];
			aDataInput.read(buffer);

			try (ObjectInputStream oos = new ObjectInputStream(new ByteArrayInputStream(buffer)))
			{
				return oos.readObject();
			}
			catch (Exception e)
			{
				throw new IllegalArgumentException(e);
			}
		}

		throw new IllegalArgumentException("Unsupported type: " + aType);
	}
}
