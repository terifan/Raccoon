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
			Class type = aFieldType.isNullable() ? EntityDescriptor.TYPE_CLASSES.get(aFieldType.getContentType()) : EntityDescriptor.TYPE_VALUES.get(aFieldType.getContentType());

			value = readArray(aInput, aFieldType, 1, type);
		}
		else
		{
			value = readValue(aFieldType, aInput);
		}

		aInput.align();

		return value;
	}


	private static Object readArray(ByteArrayBuffer aInput, FieldType aFieldType, int aLevel, Class<?> aComponentType)
	{
		int len = aInput.readVar32();

		int[] dims = new int[aFieldType.getDepth() - aLevel + 1];
		dims[0] = len;

		Object array = Array.newInstance(aComponentType, dims);

		if (aLevel < aFieldType.getDepth() || aFieldType.isNullable())
		{
			boolean[] isNull = new boolean[len];

			for (int i = 0; i < len; i++)
			{
				isNull[i] = aInput.readBit() == 1;
			}

			aInput.align();

			if (aLevel < aFieldType.getDepth())
			{
				for (int i = 0; i < len; i++)
				{
					Object value = isNull[i] ? null : readArray(aInput, aFieldType, aLevel + 1, aComponentType);

					Array.set(array, i, value);
				}
			}
			else
			{
				for (int i = 0; i < len; i++)
				{
					Object value = isNull[i] ? null : readValue(aFieldType, aInput);

					Array.set(array, i, value);
				}

				aInput.align();
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

				Array.set(array, i, value);
			}

			aInput.align();
		}

		return array;
	}


	private static Object readValue(FieldType aFieldType, ByteArrayBuffer aInput)
	{
		switch (aFieldType.getContentType())
		{
			case BOOLEAN:
//				return aInput.read() != 0;
				return aInput.readBit() == 1;
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
