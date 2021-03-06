package org.terifan.raccoon.serialization;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.lang.reflect.Array;
import java.util.Date;
import org.terifan.raccoon.util.ByteArrayBuffer;


public class FieldWriter
{
	public static void writeField(FieldDescriptor aFieldType, Object aValue, ByteArrayBuffer aOutput) throws IOException
	{
		if (aFieldType.isArray())
		{
			writeArray(aValue, aOutput, aFieldType, 1);
		}
		else
		{
			writeValue(aFieldType, aValue, aOutput);
		}

		aOutput.align();
	}


	private static void writeArray(Object aValue, ByteArrayBuffer aOutput, FieldDescriptor aFieldType, int aLevel) throws IOException
	{
		int len = Array.getLength(aValue);

		aOutput.writeVar32(len);

		if (aLevel < aFieldType.getDepth() || !aFieldType.isPrimitive())
		{
			for (int i = 0; i < len; i++)
			{
				aOutput.writeBit(Array.get(aValue, i) == null);
			}

			aOutput.align();
		}

		if (aLevel < aFieldType.getDepth())
		{
			for (int i = 0; i < len; i++)
			{
				Object value = Array.get(aValue, i);

				if (value != null)
				{
					writeArray(value, aOutput, aFieldType, aLevel + 1);
				}
			}
		}
		else if (!aFieldType.isPrimitive())
		{
			for (int i = 0; i < len; i++)
			{
				Object value = Array.get(aValue, i);

				if (value != null)
				{
					writeValue(aFieldType, value, aOutput);
				}
			}

			aOutput.align();
		}
		else
		{
			for (int i = 0; i < len; i++)
			{
				writeValue(aFieldType, Array.get(aValue, i), aOutput);
			}

			aOutput.align();
		}
	}


	private static void writeValue(FieldDescriptor aFieldType, Object aValue, ByteArrayBuffer aOutput) throws IOException
	{
		switch (aFieldType.getValueType())
		{
			case BOOLEAN:
				aOutput.writeBit((Boolean)aValue);
				break;
			case BYTE:
				aOutput.writeInt8((Byte)aValue);
				break;
			case SHORT:
				aOutput.writeVar32((Short)aValue);
				break;
			case CHAR:
				aOutput.writeVar32((Character)aValue);
				break;
			case INT:
				aOutput.writeVar32((Integer)aValue);
				break;
			case LONG:
				aOutput.writeVar64((Long)aValue);
				break;
			case FLOAT:
				aOutput.writeFloat((Float)aValue);
				break;
			case DOUBLE:
				aOutput.writeDouble((Double)aValue);
				break;
			case STRING:
				String s = (String)aValue;
				aOutput.writeVar32(s.length());
				aOutput.writeString(s);
				break;
			case DATE:
				aOutput.writeVar64(((Date)aValue).getTime());
				break;
			case OBJECT:
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				try (ObjectOutputStream oos = new ObjectOutputStream(baos))
				{
					oos.writeObject(aValue);
				}
				aOutput.writeVar32(baos.size());
				aOutput.write(baos.toByteArray());
				break;
			default:
				throw new IllegalStateException("Content type not implemented: " + aFieldType.getValueType());
		}
	}
}
