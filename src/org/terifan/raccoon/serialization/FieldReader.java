package org.terifan.raccoon.serialization;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.lang.reflect.Array;
import java.util.Date;
import org.terifan.raccoon.util.ByteArrayBuffer;


public class FieldReader
{
	public static Object readField(FieldDescriptor aFieldDescriptor, ByteArrayBuffer aInput, boolean aIgnoreMissingClasses) throws IOException, ClassNotFoundException
	{
		Object value;

		if (aFieldDescriptor.isArray())
		{
			Class type = aFieldDescriptor.getTypeClass();

			value = readArray(aInput, aFieldDescriptor, 1, type, aIgnoreMissingClasses);
		}
		else
		{
			value = readValue(aFieldDescriptor, aInput, aIgnoreMissingClasses);
		}

		aInput.align();

		return value;
	}


	private static Object readArray(ByteArrayBuffer aInput, FieldDescriptor aFieldDescriptor, int aLevel, Class<?> aComponentType, boolean aIgnoreMissingClasses) throws IOException, ClassNotFoundException
	{
		int len = aInput.readVar32();

		int[] dims = new int[aFieldDescriptor.getDepth() - aLevel + 1];
		dims[0] = len;

		Object array = Array.newInstance(aComponentType, dims);

		if (aLevel < aFieldDescriptor.getDepth() || !aFieldDescriptor.isPrimitive())
		{
			boolean[] isNull = new boolean[len];

			for (int i = 0; i < len; i++)
			{
				isNull[i] = aInput.readBit() == 1;
			}

			aInput.align();

			if (aLevel < aFieldDescriptor.getDepth())
			{
				for (int i = 0; i < len; i++)
				{
					Object value = isNull[i] ? null : readArray(aInput, aFieldDescriptor, aLevel + 1, aComponentType, aIgnoreMissingClasses);

					Array.set(array, i, value);
				}
			}
			else
			{
				for (int i = 0; i < len; i++)
				{
					Object value = isNull[i] ? null : readValue(aFieldDescriptor, aInput, aIgnoreMissingClasses);

					Array.set(array, i, value);
				}

				aInput.align();
			}
		}
		else if (aFieldDescriptor.getValueType() == ValueType.BYTE && aFieldDescriptor.isPrimitive())
		{
			aInput.read((byte[])array);
		}
		else
		{
			for (int i = 0; i < len; i++)
			{
				Object value = readValue(aFieldDescriptor, aInput, aIgnoreMissingClasses);

				Array.set(array, i, value);
			}

			aInput.align();
		}

		return array;
	}


	private static Object readValue(FieldDescriptor aFieldDescriptor, ByteArrayBuffer aInput, boolean aIgnoreMissingClasses) throws IOException, ClassNotFoundException
	{
		switch (aFieldDescriptor.getValueType())
		{
			case BOOLEAN:
				return aInput.readBit() == 1;
			case BYTE:
				return (byte)aInput.readInt8();
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
				try
				{
					byte[] buffer = aInput.read(new byte[aInput.readVar32()]);

					try (ObjectInputStream oos = new ObjectInputStream(new ByteArrayInputStream(buffer)))
					{
						return oos.readObject();
					}
				}
				catch (ClassNotFoundException e)
				{
					if (aIgnoreMissingClasses)
					{
						return null;
					}
					throw e;
				}
			default:
				throw new IllegalStateException("Content type not implemented: " + aFieldDescriptor.getValueType());
		}
	}
}
