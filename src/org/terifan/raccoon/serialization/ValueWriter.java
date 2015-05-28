package org.terifan.raccoon.serialization;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Date;


class ValueWriter
{
	static void writeValue(Object aValue, DataOutputStream aOutputStream) throws IOException
	{
		Class<?> type = aValue.getClass();

		if (type == Boolean.class)
		{
			aOutputStream.writeBoolean((Boolean)aValue);
		}
		else if (type == Byte.class)
		{
			aOutputStream.writeByte((Byte)aValue);
		}
		else if (type == Short.class)
		{
			aOutputStream.writeShort((Short)aValue);
		}
		else if (type == Character.class)
		{
			aOutputStream.writeChar((Character)aValue);
		}
		else if (type == Integer.class)
		{
			aOutputStream.writeInt((Integer)aValue);
		}
		else if (type == Long.class)
		{
			aOutputStream.writeLong((Long)aValue);
		}
		else if (type == Float.class)
		{
			aOutputStream.writeFloat((Float)aValue);
		}
		else if (type == Double.class)
		{
			aOutputStream.writeDouble((Double)aValue);
		}
		else if (type == String.class)
		{
			aOutputStream.writeUTF((String)aValue);
		}
		else if (Date.class.isAssignableFrom(type))
		{
			aOutputStream.writeLong(((Date)aValue).getTime());
		}
		else
		{
			throw new IllegalArgumentException("Unsupported: " + type);
		}
	}
}
