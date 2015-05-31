package org.terifan.raccoon.serialization;

import com.oracle.jrockit.jfr.DataType;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;
import org.terifan.raccoon.util.ByteArray;


class ValueWriter
{
	static void writeValue(boolean aNullable, Object aValue, DataOutput aDataOutput) throws IOException
	{
		if (aNullable)
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
			ByteArray.writeVarInt(aDataOutput, (Short)aValue);
		}
		else if (type == Character.class)
		{
			ByteArray.writeVarInt(aDataOutput, (Character)aValue);
		}
		else if (type == Integer.class)
		{
			ByteArray.writeVarInt(aDataOutput, (Integer)aValue);
		}
		else if (type == Long.class)
		{
			ByteArray.writeVarLong(aDataOutput, (Long)aValue);
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
			byte[] buf = ByteArray.encodeUTF8(s);
			ByteArray.writeVarInt(aDataOutput, s.length());
			aDataOutput.write(buf);
		}
		else if (Date.class.isAssignableFrom(type))
		{
			ByteArray.writeVarLong(aDataOutput, ((Date)aValue).getTime());
		}
		else
		{
			throw new IllegalArgumentException("Unsupported: " + type);
		}
	}
}
