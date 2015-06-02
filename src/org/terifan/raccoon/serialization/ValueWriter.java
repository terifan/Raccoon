package org.terifan.raccoon.serialization;

import java.util.Date;
import org.terifan.raccoon.util.ByteArrayBuffer;


class ValueWriter
{
	static void writeValue(Object aValue, ByteArrayBuffer aDataOutput)
	{
		Class<?> type = aValue.getClass();

		if (type == Boolean.class)
		{
			aDataOutput.writeBit(((Boolean)aValue) ? 1 : 0);
		}
		else if (type == Byte.class)
		{
			aDataOutput.write((Byte)aValue);
		}
		else if (type == Short.class)
		{
			aDataOutput.writeVar32((Short)aValue);
		}
		else if (type == Character.class)
		{
			aDataOutput.writeVar32((Character)aValue);
		}
		else if (type == Integer.class)
		{
			aDataOutput.writeVar32((Integer)aValue);
		}
		else if (type == Long.class)
		{
			aDataOutput.writeVar64((Long)aValue);
		}
		else if (type == Float.class)
		{
			aDataOutput.writeFloat((Float)aValue);
		}
		else if (type == Double.class)
		{
			aDataOutput.writeDouble((Double)aValue);
		}
		else if (type == String.class)
		{
			String s = (String)aValue;
			aDataOutput.writeVar32(s.length());
			aDataOutput.writeString(s);
		}
		else if (Date.class.isAssignableFrom(type))
		{
			aDataOutput.writeVar64(((Date)aValue).getTime());
		}
		else
		{
			throw new IllegalArgumentException("Unsupported: " + type);
		}
	}
}
