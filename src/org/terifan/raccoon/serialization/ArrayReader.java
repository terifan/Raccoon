package org.terifan.raccoon.serialization;

import java.io.DataInputStream;
import java.io.IOException;
import java.lang.reflect.Array;


class ArrayReader
{
	static Object readArray(FieldType aTypeInfo, int aLevel, int aDepth, DataInputStream aInputStream) throws IOException
	{
		int length = aInputStream.readInt();

		int[] dims = new int[aDepth - aLevel + 1];
		dims[0] = length;

		Object array = Array.newInstance(aTypeInfo.type, dims);

		if (aLevel == aDepth && aTypeInfo.type == Byte.TYPE)
		{
			aInputStream.readFully((byte[])array);
		}
		else if (length > 0)
		{
			boolean hasNulls = aInputStream.readBoolean();
			byte[] bitmap = null;

			if (hasNulls)
			{
				bitmap = new byte[(length + 7) / 8];
				aInputStream.readFully(bitmap);
			}

			for (int i = 0; i < length; i++)
			{
				Object value = null;
				if (!hasNulls || ((bitmap[i >> 3] & (128 >> (i & 7))) == 0))
				{
					if (aLevel == aDepth)
					{
						value = ValueReader.readValue(aTypeInfo.type, aInputStream);
					}
					else
					{
						value = readArray(aTypeInfo, aLevel + 1, aDepth, aInputStream);
					}
				}
				Array.set(array, i, value);
			}
		}

		return array;
	}
}
