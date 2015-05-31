package org.terifan.raccoon.serialization;

import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Array;
import org.terifan.raccoon.util.ByteArray;
import org.terifan.raccoon.util.Log;


class ArrayWriter
{
	static void writeArray(FieldType aFieldType, Object aArray, int aLevel, int aDepth, DataOutput aDataOutput) throws IOException, IllegalAccessException
	{
		assert aLevel <= aDepth : aLevel+" <= "+aDepth;

		if (aArray == null)
		{
			aDataOutput.writeBoolean(true);
			return;
		}

		aDataOutput.writeBoolean(false);

		int length = Array.getLength(aArray);

		ByteArray.writeVarInt(aDataOutput, length);

		if (aLevel == aDepth)
		{
			for (int i = 0; i < length; i++)
			{
				ValueWriter.writeValue(aFieldType.nullable, Array.get(aArray, i), aDataOutput);
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
