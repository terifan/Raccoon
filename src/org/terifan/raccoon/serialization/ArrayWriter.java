package org.terifan.raccoon.serialization;

import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Array;
import org.terifan.raccoon.util.Log;


class ArrayWriter
{
	static void writeArray(FieldType aTypeInfo, Object aArray, int aLevel, int aDepth, DataOutput aDataOutput) throws IOException, IllegalAccessException
	{
		assert aLevel <= aDepth : aLevel+" <= "+aDepth;

		if (aArray == null)
		{
			aDataOutput.writeBoolean(true);
			return;
		}

		int length = Array.getLength(aArray);

		aDataOutput.writeBoolean(false);
		aDataOutput.writeInt(length);

		if (aLevel == aDepth)
		{
			writeArrayContents(aTypeInfo, aArray, aDataOutput);
		}
		else
		{
			for (int i = 0; i < length; i++)
			{
				Object value = Array.get(aArray, i);

				if (value != null)
				{
					writeArray(aTypeInfo, value, aLevel + 1, aDepth, aDataOutput);
				}
			}
		}
	}


	private static void writeArrayContents(FieldType aTypeInfo, Object aArray, DataOutput aOutputStream) throws IOException, IllegalAccessException
	{
		int length = Array.getLength(aArray);

		for (int i = 0; i < length; i++)
		{
			ValueWriter.writeValue(aTypeInfo, Array.get(aArray, i), aOutputStream);
		}
	}
}
