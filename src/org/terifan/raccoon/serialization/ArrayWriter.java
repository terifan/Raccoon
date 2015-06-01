package org.terifan.raccoon.serialization;

import java.io.IOException;
import java.lang.reflect.Array;
import org.terifan.raccoon.util.ByteArrayBuffer;


class ArrayWriter
{
	static void writeArray(FieldType aFieldType, Object aArray, int aLevel, int aDepth, ByteArrayBuffer aDataOutput) throws IOException, IllegalAccessException
	{
		assert aLevel <= aDepth : aLevel+" <= "+aDepth;

		if (aArray == null)
		{
			aDataOutput.writeBit(1);
			return;
		}

		aDataOutput.writeBit(0);

		int length = Array.getLength(aArray);

		aDataOutput.writeVar32(length);

		if (aLevel == aDepth)
		{
			if (aFieldType.nullable)
			{
				for (int i = 0; i < length; i++)
				{
					aDataOutput.writeBit(Array.get(aArray, i) == null ? 1 : 0);
				}
			}
			for (int i = 0; i < length; i++)
			{
				if (Array.get(aArray, i) != null)
				{
					ValueWriter.writeValue(Array.get(aArray, i), aDataOutput);
				}
			}
		}
		else
		{
			for (int i = 0; i < length; i++)
			{
				writeArray(aFieldType, Array.get(aArray, i), aLevel + 1, aDepth, aDataOutput);
			}
		}
	}
}
