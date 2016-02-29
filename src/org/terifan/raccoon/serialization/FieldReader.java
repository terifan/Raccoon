package org.terifan.raccoon.serialization;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.lang.reflect.Array;
import java.util.Date;
import org.terifan.raccoon.util.ByteArrayBuffer;


class FieldReader
{
	static Object readField(FieldType aFieldType, ByteArrayBuffer aInput)
	{
		Object value;

		if (aFieldType.isArray())
		{
			value = readArray(aInput, aFieldType);
		}
		else
		{
			value = readValue(aFieldType, aInput);
		}

		return value;
	}


	private static Object readArray(ByteArrayBuffer aInput, FieldType aFieldType) throws NegativeArraySizeException, IllegalArgumentException, ArrayIndexOutOfBoundsException
	{
		int len = aInput.readVar32();
		
		Object array;
		
		if (aFieldType.getField() == null)
		{
			array = null;
		}
		else
		{
			array = Array.newInstance(aFieldType.getField().getType().getComponentType(), len);
		}
		
		if (aFieldType.isNullable())
		{
			boolean[] isNull = new boolean[len];

			for (int i = 0; i < len; i++)
			{
				isNull[i] = aInput.readBit() == 1;
			}
			
			aInput.align();

			for (int i = 0; i < len; i++)
			{
				Object value = isNull[i] ? null : readValue(aFieldType, aInput);

				if (array != null)
				{
					Array.set(array, i, value);
				}
			}
		}
		else if (aFieldType.getContentType() == ContentType.BYTE)
		{
			aInput.read((byte[])array);
		}
		else
		{
			for (int i = 0; i < len; i++)
			{
				Object value = readValue(aFieldType, aInput);
				
				if (array != null)
				{
					Array.set(array, i, value);
				}
			}
		}
		
		return array;
	}

	
	private static Object readValue(FieldType aFieldType, ByteArrayBuffer aInput)
	{
		switch (aFieldType.getContentType())
		{
			case BOOLEAN:
				return aInput.read() != 0;
			case BYTE:
				return (byte)aInput.read();
			case SHORT:
				return (short)aInput.readVar32();
			case CHAR:
				return (char)aInput.readVar32();
			case INT:
				return aInput.readVar32();
			case LONG:
				return aInput.readVar64();
			case FLOAT:
				return aInput.readFloat();
			case DOUBLE:
				return aInput.readDouble();
			case STRING:
				return aInput.readString(aInput.readVar32());
			case DATE:
				return new Date(aInput.readVar64());
			case OBJECT:
				byte[] buffer = new byte[aInput.readVar32()];
				aInput.read(buffer);

				try (ObjectInputStream oos = new ObjectInputStream(new ByteArrayInputStream(buffer)))
				{
					return oos.readObject();
				}
				catch (Exception e)
				{
					throw new IllegalArgumentException(e);
				}
		}

		throw new IllegalArgumentException("Unsupported type: " + aFieldType);
	}
}
