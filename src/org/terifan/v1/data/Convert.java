package org.terifan.v1.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import org.terifan.v1.io.ByteArray;


public class Convert
{
	private final static char[] HEX = {'0','1','2','3','4','5','6','7','8','9','A','B','C','D','E','F'};


	private Convert()
	{
	}


	public static String toHexString(int... aInput)
	{
		StringBuilder sb = new StringBuilder();
		for (int v : aInput)
		{
			for (int i = 28; i >= 0; i-=4)
			{
				sb.append(HEX[0xf & (v >>> i)]);
			}
		}
		return sb.toString();
	}


	public static String toHexString(long... aInput)
	{
		StringBuilder sb = new StringBuilder();
		for (long v : aInput)
		{
			for (int i = 60; i >= 0; i-=4)
			{
				sb.append(HEX[(int)(0xf & (v >>> i))]);
			}
		}
		return sb.toString();
	}


	public static int [] toInts(byte [] aInput)
	{
		return toInts(aInput, new int[aInput.length/4]);
	}


	public static int [] toInts(byte [] aInput, int [] aOutput)
	{
		return toInts(aInput, 0, aOutput, 0, aOutput.length);
	}


	public static int [] toInts(byte [] aInput, int aInputOffset, int aOutputLength)
	{
		return toInts(aInput, aInputOffset, new int[aOutputLength], 0, aOutputLength);
	}


	public static int [] toInts(byte [] aInput, int aInputOffset, int [] aOutput, int aOutputOffset, int aOutputLength)
	{
		for (int i = 0; i < aOutputLength; i++)
		{
			aOutput[aOutputOffset++] = (int)(((255 & aInput[aInputOffset++]) << 24)
			                            +       ((255 & aInput[aInputOffset++]) << 16)
			                            +       ((255 & aInput[aInputOffset++]) <<  8)
			                            +       ((255 & aInput[aInputOffset++])      ));
		}

		return aOutput;
	}


	public static byte [] toBytes(int ... aInput)
	{
		return toBytes(aInput, new byte[aInput.length * 4]);
	}


	public static byte [] toBytes(long ... aInput)
	{
		return toBytes(aInput, new byte[aInput.length * 8]);
	}


	public static byte [] toBytes(int [] aInput, byte [] aOutput)
	{
		return toBytes(aInput, 0, aOutput, 0, aOutput.length / 4);
	}


	public static byte [] toBytes(long [] aInput, byte [] aOutput)
	{
		return toBytes(aInput, 0, aOutput, 0, aOutput.length / 8);
	}


	public static byte [] toBytes(int [] aInput, int aInputOffset, int aOutputLength)
	{
		return toBytes(aInput, aInputOffset, new byte[aOutputLength], 0, aOutputLength);
	}


	public static byte [] toBytes(long [] aInput, int aInputOffset, int aOutputLength)
	{
		return toBytes(aInput, aInputOffset, new byte[aOutputLength], 0, aOutputLength);
	}


	public static byte [] toBytes(int [] aInput, int aInputOffset, byte [] aOutput, int aOutputOffset, int aOutputLength)
	{
		for (int i = 0; i < aOutputLength; i++)
		{
			int v = aInput[aInputOffset++];
			aOutput[aOutputOffset++] = (byte)(v >>> 24);
			aOutput[aOutputOffset++] = (byte)(v >>  16);
			aOutput[aOutputOffset++] = (byte)(v >>   8);
			aOutput[aOutputOffset++] = (byte)(v       );
		}

		return aOutput;
	}


	public static byte [] toBytes(long [] aInput, int aInputOffset, byte [] aOutput, int aOutputOffset, int aOutputLength)
	{
		for (int i = 0; i < aOutputLength; i++)
		{
			long v = aInput[aInputOffset++];
			aOutput[aOutputOffset++] = (byte)(v >>> 56);
			aOutput[aOutputOffset++] = (byte)(v >>  48);
			aOutput[aOutputOffset++] = (byte)(v >>  40);
			aOutput[aOutputOffset++] = (byte)(v >>  32);
			aOutput[aOutputOffset++] = (byte)(v >>  24);
			aOutput[aOutputOffset++] = (byte)(v >>  16);
			aOutput[aOutputOffset++] = (byte)(v >>   8);
			aOutput[aOutputOffset++] = (byte)(v       );
		}

		return aOutput;
	}


	public static byte [] toBytes(char [] aInput, byte [] aOutput)
	{
		return toBytes(aInput, 0, aOutput, 0, aOutput.length/2);
	}


	public static byte [] toBytes(char [] aInput, int aInputOffset, int aNumChars)
	{
		return toBytes(aInput, aInputOffset, new byte[aNumChars], 0, aNumChars);
	}


	public static byte [] toBytes(char [] aInput, int aInputOffset, byte [] aOutput, int aOutputOffset, int aNumChars)
	{
		for (int i = 0; i < aNumChars; i++)
		{
			int v = aInput[aInputOffset++];
			aOutput[aOutputOffset++] = (byte)(v >>   8);
			aOutput[aOutputOffset++] = (byte)(v       );
		}

		return aOutput;
	}


	public static byte [] toBytes(short aInput)
	{
		byte [] buf = new byte[2];
		ByteArray.BE.putShort(buf, 0, aInput);
		return buf;
	}


	public static byte [] toBytes(char aInput)
	{
		byte [] buf = new byte[2];
		ByteArray.BE.putChar(buf, 0, aInput);
		return buf;
	}


	public static byte [] toBytes(int aInput)
	{
		byte [] buf = new byte[4];
		ByteArray.BE.putInt(buf, 0, aInput);
		return buf;
	}


	public static byte [] toBytes(long aInput)
	{
		byte [] buf = new byte[8];
		ByteArray.BE.putLong(buf, 0, aInput);
		return buf;
	}


	public static byte [] toBytes(float aInput)
	{
		byte [] buf = new byte[4];
		ByteArray.BE.putFloat(buf, 0, aInput);
		return buf;
	}


	public static byte [] toBytes(double aInput)
	{
		byte [] buf = new byte[8];
		ByteArray.BE.putDouble(buf, 0, aInput);
		return buf;
	}


	public static long toLong(byte [] aInput)
	{
		return ByteArray.BE.getLong(aInput, 0);
	}


	public static long[] toLongs(byte [] aInput)
	{
		return toLongs(aInput, 0, aInput.length / 8);
	}


	public static long[] toLongs(byte [] aInput, int aInputOffset, int aNumLongs)
	{
		return toLongs(aInput, aInputOffset, new long[aNumLongs], 0, aNumLongs);
	}


	public static long[] toLongs(byte[] aInput, int aInputOffset, long[] aOutput, int aNumLongs)
	{
		return toLongs(aInput, aInputOffset, aOutput, 0, aNumLongs);
	}


	public static long[] toLongs(byte [] aInput, int aInputOffset, long[] aOutput, int aOutputOffset, int aNumLongs)
	{
		long[] buf = new long[aNumLongs];
		for (int i = 0, j = 0; i < aNumLongs; i++, j += 8)
		{
			buf[aOutputOffset + i] = ByteArray.BE.getLong(aInput, j);
		}
		return buf;
	}


	public static float[] toFloats(byte [] aInput)
	{
		return toFloats(aInput, 0, aInput.length / 8);
	}


	public static float[] toFloats(byte [] aInput, int aInputOffset, int aNumFloats)
	{
		return toFloats(aInput, aInputOffset, new float[aNumFloats], 0, aNumFloats);
	}


	public static float[] toFloats(byte[] aInput, int aInputOffset, float[] aOutput, int aNumFloats)
	{
		return toFloats(aInput, aInputOffset, aOutput, 0, aNumFloats);
	}


	public static float[] toFloats(byte [] aInput, int aInputOffset, float[] aOutput, int aOutputOffset, int aNumFloats)
	{
		float[] buf = new float[aNumFloats];
		for (int i = 0, j = 0; i < aNumFloats; i++, j += 4)
		{
			buf[aOutputOffset + i] = ByteArray.BE.getFloat(aInput, j);
		}
		return buf;
	}


	public static double[] toDoubles(byte [] aInput)
	{
		return toDoubles(aInput, 0, aInput.length / 8);
	}


	public static double[] toDoubles(byte [] aInput, int aInputOffset, int aNumDoubles)
	{
		return toDoubles(aInput, aInputOffset, new Double[aNumDoubles], 0, aNumDoubles);
	}


	public static double[] toDoubles(byte[] aInput, int aInputOffset, Double[] aOutput, int aNumDoubles)
	{
		return toDoubles(aInput, aInputOffset, aOutput, 0, aNumDoubles);
	}


	public static double[] toDoubles(byte [] aInput, int aInputOffset, Double[] aOutput, int aOutputOffset, int aNumDoubles)
	{
		double[] buf = new double[aNumDoubles];
		for (int i = 0, j = 0; i < aNumDoubles; i++, j += 8)
		{
			buf[aOutputOffset + i] = ByteArray.BE.getDouble(aInput, j);
		}
		return buf;
	}


	public static long toLong(byte [] aInput, int aOffset)
	{
		return ByteArray.BE.getLong(aInput, aOffset);
	}


	/**
	 * Converts the supplied hexadecimal string into a byte array.<p>
	 */
	public static byte [] hexToBytes(String aInput)
	{
		return hexToBytes(aInput, 0, aInput.length());
	}


	public static byte [] hexToBytes(String aHex, int aOffset, int aLength)
	{
		if (aLength % 2 != 0)
		{
			throw new IllegalArgumentException("aHexString must have an even length.");
		}

		byte [] buf = new byte[aLength / 2];

		for (int i = 0; i < buf.length; i++)
		{
			int a = decodeChar(aHex.charAt(aOffset+0));
			int b = decodeChar(aHex.charAt(aOffset+1));

			buf[i] = (byte)((a << 4) + b);
			aOffset += 2;
		}

		return buf;
	}


	private static int decodeChar(char c)
	{
		if (c >= '0' && c <= '9')
		{
			return c - '0';
		}
		else if (c >= 'A' && c <= 'F')
		{
			return c - 'A' + 10;
		}
		else if (c >= 'a' && c <= 'f')
		{
			return c - 'a' + 10;
		}
		else
		{
			throw new RuntimeException("Illegal character: " + c);
		}
	}


	/**
	 * Converts the supplied byte array to a hexadecimal string.<p>
	 */
	public static String toHexString(byte [] aInput)
	{
		StringBuilder sb = new StringBuilder(aInput.length*2);
		for (byte b : aInput)
		{
			sb.append(HEX[(b & 0xF0) >> 4]).append(HEX[b & 0x0F]);
		}
		return sb.toString();
	}


	private final static String [] DIGITS = {"0","1","2","3","4","5","6","7","8","9","A","B","C","D","E","F"};


	/**
	 * Converts the supplied byte array to a hexadecimal string.<p>
	 */
	public static String toHexString(byte [] aInput, int aInputOffset, int aLength)
	{
		StringBuilder s = new StringBuilder(2*aLength);
		for (int i = 0; i < aLength; i++)
		{
			int b = aInput[aInputOffset+i] & 255;
			s.append(DIGITS[b>>4]).append(DIGITS[b&15]);
		}
		return s.toString();
	}


	/**
	 * Encodes a unicode String to UTF-8 format.
	 */
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


	/**
	 * Decodes an UTF-8 byte array to a java String.
	 */
	public static String decodeUTF8(byte [] aInput)
	{
		return decodeUTF8(aInput, 0, aInput.length);
	}


	/**
	 * Decodes an UTF-8 byte array to a java String.
	 *
	 * @param aInput
	 *  buffer containing the encoded string
	 * @param aInputOffset
	 *   offset in the buffer to start decoding
	 * @param aLength
	 *   length of the encoded string to decode
	 */
	public static String decodeUTF8(byte [] aInput, int aInputOffset, int aLength)
	{
		char [] array = new char[aLength];
		int bufOffset = 0;

		for (int i = 0, sz = aLength; i < sz;)
		{
			int c = aInput[aInputOffset + i++] & 255;

			if (c < 128) // 0xxxxxxx
			{
				array[bufOffset++] = (char)c;
			}
			else if ((c & 0xE0) == 0xC0) // 110xxxxx
			{
				array[bufOffset++] = (char)(((c & 0x1F) << 6) | (aInput[i++] & 0x3F));
			}
			else if ((c & 0xF0) == 0xE0) // 1110xxxx
			{
				array[bufOffset++] = (char)(((c & 0x0F) << 12) | ((aInput[i++] & 0x3F) << 6) | (aInput[i++] & 0x3F));
			}
			else
			{
				throw new RuntimeException("This decoder only handles 16-bit characters: c = " + c);
			}
		}

		return new String(array, 0, bufOffset);
	}


	public static void encodeUTF8(String aInput, DataOutput aOutput) throws IOException
	{
		for (int i = 0, len = aInput.length(); i < len; i++)
		{
			char c = aInput.charAt(i);
			if ((c >= 0x0000) && (c <= 0x007F))
			{
				aOutput.write(c);
			}
			else if (c > 0x07FF)
			{
				aOutput.write(0xE0 | ((c >> 12) & 0x0F));
				aOutput.write(0x80 | ((c >> 6) & 0x3F));
				aOutput.write(0x80 | ((c) & 0x3F));
			}
			else
			{
				aOutput.write(0xC0 | ((c >> 6) & 0x1F));
				aOutput.write(0x80 | ((c) & 0x3F));
			}
		}
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


	public static String toBinString(long aInput, int aLength)
	{
		StringBuilder sb = new StringBuilder(aLength);
		for (int i = aLength; --i >= 0;)
		{
			sb.append((aInput >>> i) & 1L);
		}
		return sb.toString();
	}


	public static int toInt(byte[] aInput)
	{
		return ByteArray.BE.getInt(aInput, 0);
	}


	public static short[] toShorts(byte[] aInput)
	{
		return toShorts(aInput, 0, aInput.length);
	}


	public static short[] toShorts(byte[] aInput, int aOffset, int aOutputLength)
	{
		short[] c = new short[aOutputLength];
		for (int i = 0; i < aOutputLength; i++)
		{
			c[i] = ByteArray.BE.getShort(aInput, aOffset + 2 * i);
		}
		return c;
	}


	public static char[] toChars(byte[] aInput)
	{
		return toChars(aInput, 0, aInput.length / 2);
	}


	public static char[] toChars(byte[] aInput, int aOffset, int aOutputLength)
	{
		char[] c = new char[aOutputLength];
		for (int i = 0; i < aOutputLength; i++)
		{
			c[i] = ByteArray.BE.getChar(aInput, aOffset + 2 * i);
		}
		return c;
	}


	public static byte [] toBytes(short ... aInput)
	{
		byte[] bytes = new byte[aInput.length * 2];
		for (int i = 0; i < aInput.length; i++)
		{
			ByteArray.BE.putShort(bytes, 2 * i, aInput[i]);
		}
		return bytes;
	}


	public static byte [] toBytes(char ... aInput)
	{
		byte[] bytes = new byte[aInput.length * 2];
		for (int i = 0; i < aInput.length; i++)
		{
			ByteArray.BE.putChar(bytes, 2 * i, aInput[i]);
		}
		return bytes;
	}


	public static byte [] toBytes(float ... aInput)
	{
		byte[] bytes = new byte[aInput.length * 4];
		for (int i = 0; i < aInput.length; i++)
		{
			ByteArray.BE.putFloat(bytes, 4 * i, aInput[i]);
		}
		return bytes;
	}


	public static byte [] toBytes(double ... aInput)
	{
		byte[] bytes = new byte[aInput.length * 8];
		for (int i = 0; i < aInput.length; i++)
		{
			ByteArray.BE.putDouble(bytes, 8 * i, aInput[i]);
		}
		return bytes;
	}


//	public static void main(String... args)
//	{
//		try
//		{
//			Log.out.println(toHexString(0x12345678));
//			Log.out.println(toHexString(0x123456789abcdef0l));
//		}
//		catch (Throwable e)
//		{
//			e.printStackTrace(System.out);
//		}
//	}
}
