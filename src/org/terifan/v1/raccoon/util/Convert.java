package org.terifan.v1.raccoon.util;

import java.io.DataInput;
import java.io.IOException;
import java.util.Arrays;


public class Convert
{
	public static byte [] encodeUTF8(String aInput)
	{
		byte [] array = new byte[aInput.length()];
		int ptr = 0;

		for (int i = 0, len = aInput.length(); i < len; i++)
		{
			if (ptr+3 > array.length)
			{
				array = Arrays.copyOf(array, (ptr+1)*3/2);
			}

			char c = aInput.charAt(i);
		    if ((c >= 0x0000) && (c <= 0x007F))
		    {
				array[ptr++] = (byte)c;
		    }
		    else if (c > 0x07FF)
		    {
				array[ptr++] = (byte)(0xE0 | ((c >> 12) & 0x0F));
				array[ptr++] = (byte)(0x80 | ((c >>  6) & 0x3F));
				array[ptr++] = (byte)(0x80 | ((c      ) & 0x3F));
		    }
		    else
		    {
				array[ptr++] = (byte)(0xC0 | ((c >>  6) & 0x1F));
				array[ptr++] = (byte)(0x80 | ((c      ) & 0x3F));
		    }
		}

		return Arrays.copyOf(array, ptr);
	}


	public static String decodeUTF8(DataInput aInput, int aLength) throws IOException
	{
		char[] array = new char[aLength];
		int bufOffset = 0;

		for (int i = 0; i < aLength; i++)
		{
			int c = aInput.readUnsignedByte();

			if (c < 128) // 0xxxxxxx
			{
				array[bufOffset++] = (char)c;
			}
			else if ((c & 0xE0) == 0xC0) // 110xxxxx
			{
				array[bufOffset++] = (char)(((c & 0x1F) << 6) | (aInput.readByte() & 0x3F));
			}
			else if ((c & 0xF0) == 0xE0) // 1110xxxx
			{
				array[bufOffset++] = (char)(((c & 0x0F) << 12) | ((aInput.readByte() & 0x3F) << 6) | (aInput.readByte() & 0x3F));
			}
			else
			{
				throw new RuntimeException("This decoder only handles 16-bit characters: c = " + c);
			}
		}

		return new String(array, 0, bufOffset);
	}
}
