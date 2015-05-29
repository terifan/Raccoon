package org.terifan.raccoon.serialization;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;


class ValueWriter
{
	static void writeValue(FieldType aFieldType, Object aValue, DataOutput aDataOutput) throws IOException
	{
		if (!aFieldType.primitive)
		{
			if (aValue == null)
			{
				aDataOutput.writeBoolean(true);
				return;
			}

			aDataOutput.writeBoolean(false);
		}

		Class<?> type = aValue.getClass();

		if (type == Boolean.class)
		{
			aDataOutput.writeBoolean((Boolean)aValue);
		}
		else if (type == Byte.class)
		{
			aDataOutput.writeByte((Byte)aValue);
		}
		else if (type == Short.class)
		{
			aDataOutput.writeShort((Short)aValue);
		}
		else if (type == Character.class)
		{
			aDataOutput.writeChar((Character)aValue);
		}
		else if (type == Integer.class)
		{
			aDataOutput.writeInt((Integer)aValue);
		}
		else if (type == Long.class)
		{
			aDataOutput.writeLong((Long)aValue);
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
			aDataOutput.writeUTF((String)aValue);
		}
		else if (Date.class.isAssignableFrom(type))
		{
			aDataOutput.writeLong(((Date)aValue).getTime());
		}
		else
		{
			throw new IllegalArgumentException("Unsupported: " + type);
		}
	}
}
