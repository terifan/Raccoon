package org.terifan.raccoon.serialization.old;

import java.lang.reflect.Array;
import org.terifan.raccoon.util.ByteArrayBuffer;


class ArrayWriter
{
	static void writeArray(FieldType aFieldType, Object aArray, int aLevel, int aDepth, ByteArrayBuffer aDataOutput) throws IllegalAccessException
	{
		assert aLevel <= aDepth : aLevel+" <= "+aDepth;

		if (aArray == null)
		{
			aDataOutput.write(1);
			return;
		}

		aDataOutput.write(0);

		int length = Array.getLength(aArray);

		aDataOutput.writeVar32(length);

		if (aLevel == aDepth)
		{
			if (aFieldType.isNullable())
			{
				for (int i = 0; i < length; i++)
				{
					aDataOutput.writeBit(Array.get(aArray, i) == null);
				}
				aDataOutput.align();
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