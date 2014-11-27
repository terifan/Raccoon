package org.terifan.v1.data;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;


/**
 * Utility class for reading and writing variable length integers. The encoding store
 * signed values up to 64-bits in length.
 */
public final class Varint
{
	/**
	 * Gets a value from the buffer. The buffers position is advanced.
	 *
	 * @param aBuffer
	 *   the buffer
	 * @return
	 *   the value stored
	 */
	public static long get(ByteBuffer aBuffer) throws IOException
	{
		long value = 0L;

		for (int n = 0; n < 64; n+=7)
		{
			int b = aBuffer.get();
			value |= (long)(b & 0x7F) << n;
			if ((b & 0x80) == 0)
			{
				return value;
			}
		}

		throw new IOException();
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
	public static ByteBuffer put(ByteBuffer aBuffer, long aValue)
	{
		while (true)
		{
			if ((aValue & ~0x7FL) == 0)
			{
				aBuffer.put((byte)aValue);
				return aBuffer;
			}
			else
			{
				aBuffer.put((byte)(0x80 | (aValue & 0x7FL)));
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
	public static long get(InputStream aBuffer) throws IOException
	{
		long value = 0L;

		for (int n = 0; n < 64; n+=7)
		{
			int b = aBuffer.read();
			if (b == -1)
			{
				return value;
			}
			value |= (long)(b & 0x7F) << n;
			if ((b & 0x80) == 0)
			{
				return value;
			}
		}

		throw new IOException();
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
	public static OutputStream put(OutputStream aBuffer, long aValue) throws IOException
	{
		while (true)
		{
			if ((aValue & ~0x7FL) == 0)
			{
				aBuffer.write((int)aValue);
				return aBuffer;
			}
			else
			{
				aBuffer.write((int)(0x80 | ((int)aValue & 0x7FL)));
				aValue >>>= 7;
			}
		}
	}


	/**
	 * Return the length of a varint code with the value provided.
	 *
	 * @param aValue
	 *   the value
	 * @return
	 *   length of value when encoded
	 */
	public static int length(long aValue)
	{
		if ((aValue & (0xffffffffffffffffL <<  7)) == 0) return 1;
		if ((aValue & (0xffffffffffffffffL << 14)) == 0) return 2;
		if ((aValue & (0xffffffffffffffffL << 21)) == 0) return 3;
		if ((aValue & (0xffffffffffffffffL << 28)) == 0) return 4;
		if ((aValue & (0xffffffffffffffffL << 35)) == 0) return 5;
		if ((aValue & (0xffffffffffffffffL << 42)) == 0) return 6;
		if ((aValue & (0xffffffffffffffffL << 49)) == 0) return 7;
		if ((aValue & (0xffffffffffffffffL << 56)) == 0) return 8;
		if ((aValue & (0xffffffffffffffffL << 63)) == 0) return 9;
		return 10;
	}


	public static int encodeZigZag32(final int n)
	{
		return (n << 1) ^ (n >> 31);
	}


	public static long encodeZigZag64(final long n)
	{
		return (n << 1) ^ (n >> 63);
	}


	public static int decodeZigZag32(final int n)
	{
		return (n >>> 1) ^ -(n & 1);
	}


	public static long decodeZigZag64(final long n)
	{
		return (n >>> 1) ^ -(n & 1);
	}


//	public static void main(String... args)
//	{
//		try
//		{
//			ByteBuffer bb = ByteBuffer.allocate(1000);
//
//			java.util.Random rnd = new java.util.Random(1);
//
//			long[][] input = new long[3][9];
//
//			for (int test = 0; test < 1000000; test++)
//			{
//				bb.position(0);
//
//				for (int round = 0; round < 3; round++)
//				{
//					for (int shift = 0; shift < 9; shift++)
//					{
//						long in = input[round][shift] = (rnd.nextBoolean() ? -1 : 1) * (rnd.nextLong() & ((1L << (7 * (1 + shift)))-1));
//						Log.out.println(in);
//						Varint.put(bb, in);
//					}
//				}
//
//				bb.position(0);
//
//				for (int round = 0; round < 3; round++)
//				{
//					for (int shift = 0; shift < 9; shift++)
//					{
//						long out = Varint.get(bb);
//
//						if (out != input[round][shift])
//						{
//							System.out.println("error in " + test + ", " + round + ", " + shift + ": in: "+input[round][shift]+", out: "+out);
//							break;
//						}
//					}
//				}
//			}
//		}
//		catch (Exception e)
//		{
//			e.printStackTrace(Log.out);
//		}
//	}
}