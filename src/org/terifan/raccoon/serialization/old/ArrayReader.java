package org.terifan.raccoon.serialization.old;

import java.io.IOException;
import java.lang.reflect.Array;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;


class ArrayReader
{
	static Object readArray(FieldType aFieldType, int aLevel, int aDepth, ByteArrayBuffer aDataInput) throws IOException
	{
		if (aDataInput.read() != 0)
		{
			return null;
		}

		int length = aDataInput.readVar32();

		int[] dims = new int[aDepth - aLevel + 1];
		dims[0] = length;

		Object array = Array.newInstance(aFieldType.getType(), dims);

		if (aLevel == aDepth && aFieldType.getType() == Byte.TYPE)
		{
			aDataInput.read((byte[])array);
		}
		else if (length > 0)
		{
			if (aLevel == aDepth)
			{
				boolean[] nulls = new boolean[length];
				if (aFieldType.isNullable())
				{
					for (int i = 0; i < length; i++)
					{
						nulls[i] = aDataInput.readBit() != 0;
					}
					aDataInput.align();
				}
				for (int i = 0; i < length; i++)
				{
					Object value;
					if (nulls[i])
					{
						value = null;
					}
					else
					{
						value = ValueReader.readValue(aFieldType.getType(), aDataInput);
					}
					Array.set(array, i, value);
				}
			}
			else
			{
				for (int i = 0; i < length; i++)
				{
					Object value = readArray(aFieldType, aLevel + 1, aDepth, aDataInput);
					Array.set(array, i, value);
				}
			}
		}

		return array;
	}
}