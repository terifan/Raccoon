package org.terifan.raccoon.serialization;

import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Array;


class ArrayWriter
{
	static void writeArray(FieldType aTypeInfo, Object aArray, int aLevel, int aDepth, DataOutputStream aOutputStream) throws IOException, IllegalAccessException
	{
		assert aLevel <= aDepth : aLevel+" <= "+aDepth;

		int length = Array.getLength(aArray);

		if (aLevel == aDepth)
		{
			writeArrayContents(aTypeInfo, aArray, length, aOutputStream);
		}
		else
		{
			writeArrayHeader(aArray, length, aOutputStream);

			for (int i = 0; i < length; i++)
			{
				Object value = Array.get(aArray, i);

				if (value != null)
				{
					writeArray(aTypeInfo, value, aLevel + 1, aDepth, aOutputStream);
				}
			}
		}
	}


	private static void writeArrayContents(FieldType aTypeInfo, Object aArray, int aLength, DataOutputStream aOutputStream) throws IOException, IllegalAccessException
	{
		if (aTypeInfo.primitive)
		{
			aOutputStream.writeInt(aLength);

			if (aTypeInfo.type == Byte.TYPE)
			{
				aOutputStream.write((byte[])aArray);
			}
			else
			{
				for (int i = 0; i < aLength; i++)
				{
					ValueWriter.writeValue(Array.get(aArray, i), aOutputStream);
				}
			}
		}
		else if (aLength > 0)
		{
			writeArrayHeader(aArray, aLength, aOutputStream);

			for (int i = 0; i < aLength; i++)
			{
				Object value = Array.get(aArray, i);

				if (value != null)
				{
					ValueWriter.writeValue(value, aOutputStream);
				}
			}
		}
	}


	private static void writeArrayHeader(Object aArray, int aLength, DataOutputStream aOutputStream) throws IOException
	{
		byte[] bitmap = null;

		for (int i = 0; i < aLength; i++)
		{
			if (Array.get(aArray, i) == null)
			{
				if (bitmap == null)
				{
					bitmap = new byte[(aLength + 7) / 8];
				}
				bitmap[i >> 3] |= 128 >> (i & 7);
			}
		}

		aOutputStream.writeInt(aLength);
		aOutputStream.writeBoolean(bitmap != null);

		if (bitmap != null)
		{
			aOutputStream.write(bitmap);
		}
	}
}
