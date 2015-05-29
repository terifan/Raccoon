package org.terifan.raccoon.serialization;

import java.io.DataInput;
import java.io.IOException;
import java.lang.reflect.Array;
import org.terifan.raccoon.util.Log;


class ArrayReader
{
	static Object readArray(FieldType aTypeInfo, int aLevel, int aDepth, DataInput aDataInput) throws IOException
	{
		if (aDataInput.readBoolean())
		{
			return null;
		}

		int length = aDataInput.readInt();

		int[] dims = new int[aDepth - aLevel + 1];
		dims[0] = length;

		Object array = Array.newInstance(aTypeInfo.type, dims);

		if (aLevel == aDepth && aTypeInfo.type == Byte.TYPE)
		{
			aDataInput.readFully((byte[])array);
		}
		else if (length > 0)
		{
			for (int i = 0; i < length; i++)
			{
				Object value = null;

				if (aLevel == aDepth)
				{
					value = ValueReader.readValue(aTypeInfo, aDataInput);
				}
				else
				{
					value = readArray(aTypeInfo, aLevel + 1, aDepth, aDataInput);
				}

				Array.set(array, i, value);
			}
		}

		return array;
	}
}
