package org.terifan.raccoon.serialization;

import java.io.DataInput;
import java.io.IOException;
import java.lang.reflect.Array;
import org.terifan.raccoon.util.ByteArray;
import org.terifan.raccoon.util.Log;


class ArrayReader
{
	static Object readArray(FieldType aFieldType, int aLevel, int aDepth, DataInput aDataInput) throws IOException
	{
		if (aDataInput.readBoolean())
		{
			return null;
		}

		int length = ByteArray.readVarInt(aDataInput);

		int[] dims = new int[aDepth - aLevel + 1];
		dims[0] = length;

		Object array = Array.newInstance(aFieldType.type, dims);

		if (aLevel == aDepth && aFieldType.type == Byte.TYPE)
		{
			aDataInput.readFully((byte[])array);
		}
		else if (length > 0)
		{
			for (int i = 0; i < length; i++)
			{
				Object value;

				if (aLevel == aDepth)
				{
					value = ValueReader.readValue(aFieldType, aDataInput);
				}
				else
				{
					value = readArray(aFieldType, aLevel + 1, aDepth, aDataInput);
				}

				Array.set(array, i, value);
			}
		}

		return array;
	}
}
