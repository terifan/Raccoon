package org.terifan.raccoon.util;

import java.io.DataInput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;


public class ByteArray
{
	/**
	 * Puts a value into the buffer. The buffers position is advanced.
	 *
	 * @param aBuffer
	 *   the buffer
	 * @param aValue
	 *   the value to store in the buffer
	 * @return
	 *   the buffer
	 */
	public static ByteBuffer putVarLong(ByteBuffer aBuffer, long aValue)
	{
		while (true)
		{
			if ((aValue & ~127L) == 0)
			{
				aBuffer.put((byte)aValue);
				return aBuffer;
			}
			else
			{
				aBuffer.put((byte)(128 | (aValue & 127L)));
				aValue >>>= 7;
			}
		}
	}


	/**
	 * Puts a value into the buffer. The buffers position is advanced.
	 *
	 * @param aBuffer
	 *   the buffer
	 * @param aValue
	 *   the value to store in the buffer
	 * @return
	 *   the buffer
	 */
	public static OutputStream putVarLong(OutputStream aBuffer, long aValue) throws IOException
	{
		while (true)
		{
			if ((aValue & ~127L) == 0)
			{
				aBuffer.write((int)aValue);
				return aBuffer;
			}
			else
			{
				aBuffer.write((int)(128 | ((int)aValue & 127L)));
				aValue >>>= 7;
			}
		}
	}


	/**
	 * Gets a value from the buffer. The buffers position is advanced.
	 *
	 * @param aBuffer
	 *   the buffer
	 * @return
	 *   the value stored
	 */
	public static long getVarLong(ByteBuffer aBuffer) throws IOException
	{
		long value = 0L;
		for (int n = 0; n < 64; n += 7)
		{
			int b = aBuffer.get();
			value |= (long)(b & 127) << n;
			if ((b & 128) == 0)
			{
				return value;
			}
		}
		throw new IOException();
	}


	/**
	 * Gets a value from the buffer. The buffers position is advanced.
	 *
	 * @param aBuffer
	 *   the buffer
	 * @return
	 *   the value stored
	 */
	public static long getVarLong(InputStream aBuffer) throws IOException
	{
		long value = 0L;
		for (int n = 0; n < 64; n += 7)
		{
			int b = aBuffer.read();
			if (b == -1)
			{
				return value;
			}
			value |= (long)(b & 127) << n;
			if ((b & 128) == 0)
			{
				return value;
			}
		}
		throw new IOException();
	}


	public static int encodeZigZag32(final int n)
	{
		return (n << 1) ^ (n >> 31);
	}


	public static long decodeZigZag64(final long n)
	{
		return (n >>> 1) ^ -(n & 1);
	}


	public static long encodeZigZag64(final long n)
	{
		return (n << 1) ^ (n >> 63);
	}


	public static int decodeZigZag32(final int n)
	{
		return (n >>> 1) ^ -(n & 1);
	}
	private ByteArray()
	{
	}


	public static long getUnsignedInt(byte [] aBuffer, int aPosition)
	{
		return getInt(aBuffer, aPosition) & 0xFFFFFFFFL;
	}


	public static void put(byte [] aBuffer, int aPosition, byte [] aSrcBuffer)
	{
		System.arraycopy(aSrcBuffer, 0, aBuffer, aPosition, aSrcBuffer.length);
	}


	public static int getInt(byte [] aBuffer, int aPosition)
	{
		return (int)(((aBuffer[aPosition++] & 255) << 24)
				   + ((aBuffer[aPosition++] & 255) << 16)
				   + ((aBuffer[aPosition++] & 255) <<  8)
				   + ((aBuffer[aPosition  ] & 255)      ));
	}


	public static long getLong(byte [] aBuffer, int aPosition)
	{
		return (((long)(aBuffer[aPosition++]      ) << 56)
			  + ((long)(aBuffer[aPosition++] & 255) << 48)
			  + ((long)(aBuffer[aPosition++] & 255) << 40)
			  + ((long)(aBuffer[aPosition++] & 255) << 32)
			  + ((long)(aBuffer[aPosition++] & 255) << 24)
			  + (      (aBuffer[aPosition++] & 255) << 16)
			  + (      (aBuffer[aPosition++] & 255) <<  8)
			  + (      (aBuffer[aPosition  ] & 255)      ));
	}


	public static void putInt(byte [] aBuffer, int aPosition, int aValue)
	{
		aBuffer[aPosition++] = (byte)(aValue >>> 24);
		aBuffer[aPosition++] = (byte)(aValue >>  16);
		aBuffer[aPosition++] = (byte)(aValue >>   8);
		aBuffer[aPosition  ] = (byte)(aValue       );
	}


	public static void putLong(byte [] aBuffer, int aPosition, long aValue)
	{
		aBuffer[aPosition++] = (byte)(aValue >>> 56);
		aBuffer[aPosition++] = (byte)(aValue >>  48);
		aBuffer[aPosition++] = (byte)(aValue >>  40);
		aBuffer[aPosition++] = (byte)(aValue >>  32);
		aBuffer[aPosition++] = (byte)(aValue >>  24);
		aBuffer[aPosition++] = (byte)(aValue >>  16);
		aBuffer[aPosition++] = (byte)(aValue >>   8);
		aBuffer[aPosition  ] = (byte)(aValue       );
	}


	public static boolean equals(byte[] aArrayA, int aOffsetA, byte[] aArrayB, int aOffsetB, int aLength)
	{
		if (aArrayA.length < aOffsetA + aLength)
		{
			return false;
		}
		if (aArrayB.length < aOffsetB + aLength)
		{
			return false;
		}
		for (int i = 0; i < aLength; i++)
		{
			if (aArrayA[aOffsetA++] != aArrayB[aOffsetB++])
			{
				return false;
			}
		}
		return true;
	}


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