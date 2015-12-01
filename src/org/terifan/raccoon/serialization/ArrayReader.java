package org.terifan.raccoon.serialization;

import java.io.IOException;
import java.lang.reflect.Array;
import org.terifan.raccoon.util.ByteArrayBuffer;


class ArrayReader
{
	static Object readArray(FieldType aFieldType, int aLevel, int aDepth, ByteArrayBuffer aDataInput) throws IOException
	{
		if (aDataInput.readBit() == 1)
		{
			return null;
		}

		int length = aDataInput.readVar32();

		int[] dims = new int[aDepth - aLevel + 1];
		dims[0] = length;

		Object array = Array.newInstance(aFieldType.type, dims);

		if (aLevel == aDepth && aFieldType.type == Byte.TYPE)
		{
			aDataInput.read((byte[])array);
		}
		else if (length > 0)
		{
			if (aLevel == aDepth)
			{
				boolean[] nulls = new boolean[length];
				if (aFieldType.nullable)
				{
					for (int i = 0; i < length; i++)
					{
						nulls[i] = aDataInput.readBit() == 1;
					}
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
						value = ValueReader.readValue(aFieldType.type, aDataInput);
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
