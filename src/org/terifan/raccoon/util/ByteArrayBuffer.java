package org.terifan.raccoon.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;


public final class ByteArrayBuffer extends InputStream
{
	private final static boolean FORCE_FIXED = false;
	private final static int NO_LIMIT = Integer.MAX_VALUE;

	private byte readBuffer[] = new byte[8];
	private byte[] mBuffer;
	private int mOffset;
	private int mLimit;
	private boolean mLocked;
	private int mWriteBitsToGo;
	private int mBitBuffer;
	private int mReadBitCount;


	/**
	 * Read/write from a variable length buffer. Writing beyond the buffer will grow the buffer while reading will cause an exception.
	 */
	public ByteArrayBuffer(int aInitialSize)
	{
		mBuffer = new byte[aInitialSize];
		mLimit = NO_LIMIT;
		mBitBuffer = 0;
		mWriteBitsToGo = 8;
	}


	/**
	 * Read/write from a fixed size buffer. Writing beyond the buffer will cause an exception.
	 */
	public ByteArrayBuffer(byte[] aBuffer)
	{
		wrap(aBuffer);
	}


	public int capacity()
	{
		return mBuffer.length;
	}


	public ByteArrayBuffer capacity(int aNewLength)
	{
		mBuffer = Arrays.copyOfRange(mBuffer, 0, aNewLength);
		mOffset = Math.min(mOffset, aNewLength);
		return this;
	}


	public ByteArrayBuffer limit(int aLimit)
	{
		mLimit = aLimit;
		return this;
	}


	public int limit()
	{
		return mLimit;
	}


	public ByteArrayBuffer position(int aOffset)
	{
		align();
		mOffset = aOffset;
		return this;
	}


	public int position()
	{
		return mOffset;
	}


	public ByteArrayBuffer skip(int aLength)
	{
		flushBits();
		mOffset += aLength;
		return this;
	}


	public int remaining()
	{
		return mLimit == NO_LIMIT ? capacity() - position() : mLimit - position();
	}


	private ByteArrayBuffer ensureCapacity(int aIncrement)
	{
		if (mBuffer.length < mOffset + aIncrement)
		{
			if (mLocked)
			{
				throw new EOFException("Buffer capacity cannot be increased, capacity " + mBuffer.length + ", offset " + mOffset + ", increment " + aIncrement);
			}

			mBuffer = Arrays.copyOfRange(mBuffer, 0, (mOffset + aIncrement) * 3 / 2);
		}

		return this;
	}


	public ByteArrayBuffer trim()
	{
		if (mBuffer.length != mOffset)
		{
			mBuffer = Arrays.copyOfRange(mBuffer, 0, mOffset);
		}
		return this;
	}


	public ByteArrayBuffer crop()
	{
		mBuffer = Arrays.copyOfRange(mBuffer, mOffset, mBuffer.length);
		position(0);
		mLimit = NO_LIMIT;
		return this;
	}


	public byte[] array()
	{
		flushBits();

		return mBuffer;
	}


	public ByteArrayBuffer wrap(byte[] aBuffer)
	{
		if (aBuffer == null)
		{
			throw new IllegalArgumentException("Buffer provided is null.");
		}

		mBuffer = aBuffer;
		mLocked = true;
		mLimit = NO_LIMIT;
		mBitBuffer = 0;
		mWriteBitsToGo = 8;
		return this;
	}


	@Override
	public int read()
	{
		if (mOffset >= mBuffer.length || mOffset >= mLimit)
		{
			return -1;
//			throw new EOFException("Reading beyond end of buffer, capacity " + mBuffer.length + ", offset " + mOffset + ", limit " + mLimit);
		}

		align();

		return 0xff & mBuffer[mOffset++];
	}


	public ByteArrayBuffer write(int aByte)
	{
		align();

		if (mOffset >= mBuffer.length)
		{
			ensureCapacity(1);
		}

		mBuffer[mOffset++] = (byte)aByte;
		return this;
	}


	public int readVar32() throws IOException
	{
		if (FORCE_FIXED)
		{
			return readInt32();
		}

		for (int n = 0, value = 0; n < 32; n += 7)
		{
			int b = read();
			value |= (b & 127) << n;
			if (b < 128)
			{
				return decodeZigZag32(value);
			}
		}

		throw new IOException("Variable int exceeds maximum length");
	}


	public ByteArrayBuffer writeVar32(int aValue)
	{
		if (FORCE_FIXED)
		{
			writeInt32(aValue);
			return this;
		}

		aValue = encodeZigZag32(aValue);

		while (true)
		{
			if ((aValue & ~127) == 0)
			{
				write(aValue);
				return this;
			}
			else
			{
				write(128 | (aValue & 127));
				aValue >>>= 7;
			}
		}
	}


	public long readVar64() throws IOException
	{
		if (FORCE_FIXED)
		{
			return readInt64();
		}

		for (long n = 0, value = 0; n < 64; n += 7)
		{
			int b = read();
			value |= (long)(b & 127) << n;
			if ((b & 128) == 0)
			{
				return decodeZigZag64(value);
			}
		}

		throw new IOException("Variable long exceeds maximum length");
	}


	public ByteArrayBuffer writeVar64(long aValue)
	{
		if (FORCE_FIXED)
		{
			writeInt64(aValue);
			return this;
		}

		aValue = encodeZigZag64(aValue);

		while (true)
		{
			if ((aValue & ~127L) == 0)
			{
				write((int)aValue);
				return this;
			}
			else
			{
				write((int)(128 | ((int)aValue & 127L)));
				aValue >>>= 7;
			}
		}
	}


	@Override
	public int read(byte[] aBuffer)
	{
		return read(aBuffer, 0, aBuffer.length);
	}


	@Override
	public int read(byte[] aBuffer, int aOffset, int aLength)
	{
		align();

		int len = Math.min(aLength, remaining());

		System.arraycopy(mBuffer, mOffset, aBuffer, aOffset, len);
		mOffset += len;

		return len;
	}


	public ByteArrayBuffer write(byte[] aBuffer)
	{
		return write(aBuffer, 0, aBuffer.length);
	}


	public ByteArrayBuffer write(byte[] aBuffer, int aOffset, int aLength)
	{
		align();
		ensureCapacity(aLength);

		System.arraycopy(aBuffer, aOffset, mBuffer, mOffset, aLength);
		mOffset += aLength;
		return this;
	}


	public short readInt16()
	{
		align();
		int ch1 = read();
		int ch2 = read();
		return (short)((ch1 << 8) + ch2);
	}


	public ByteArrayBuffer writeInt16(short aValue)
	{
		align();
		ensureCapacity(2);
		mBuffer[mOffset++] = (byte)(aValue >> 8);
		mBuffer[mOffset++] = (byte)(aValue);
		return this;
	}


	public int readInt32()
	{
		align();
		int ch1 = read();
		int ch2 = read();
		int ch3 = read();
		int ch4 = read();
		return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + ch4);
	}


	public ByteArrayBuffer writeInt32(int aValue)
	{
		align();
		ensureCapacity(4);
		mBuffer[mOffset++] = (byte)(aValue >>> 24);
		mBuffer[mOffset++] = (byte)(aValue >> 16);
		mBuffer[mOffset++] = (byte)(aValue >> 8);
		mBuffer[mOffset++] = (byte)(aValue);
		return this;
	}


	public long readInt64()
	{
		align();
		read(readBuffer, 0, 8);
		return (((long)readBuffer[0] << 56)
			+ ((long)(readBuffer[1] & 255) << 48)
			+ ((long)(readBuffer[2] & 255) << 40)
			+ ((long)(readBuffer[3] & 255) << 32)
			+ ((long)(readBuffer[4] & 255) << 24)
			+ ((readBuffer[5] & 255) << 16)
			+ ((readBuffer[6] & 255) << 8)
			+ (readBuffer[7] & 255));
	}


	public ByteArrayBuffer writeInt64(long aValue)
	{
		align();
		ensureCapacity(8);
		mBuffer[mOffset++] = (byte)(aValue >>> 56);
		mBuffer[mOffset++] = (byte)(aValue >> 48);
		mBuffer[mOffset++] = (byte)(aValue >> 40);
		mBuffer[mOffset++] = (byte)(aValue >> 32);
		mBuffer[mOffset++] = (byte)(aValue >> 24);
		mBuffer[mOffset++] = (byte)(aValue >> 16);
		mBuffer[mOffset++] = (byte)(aValue >> 8);
		mBuffer[mOffset++] = (byte)(aValue);
		return this;
	}


	public float readFloat()
	{
		return Float.intBitsToFloat(readInt32());
	}


	public ByteArrayBuffer writeFloat(float aFloat)
	{
		return writeInt32(Float.floatToIntBits(aFloat));
	}


	public double readDouble()
	{
		return Double.longBitsToDouble(readInt64());
	}


	public ByteArrayBuffer writeDouble(double aDouble)
	{
		return writeInt64(Double.doubleToLongBits(aDouble));
	}


	public String readString(int aLength)
	{
		align();
		char[] array = new char[aLength];

		for (int i = 0, j = 0; i < aLength; i++)
		{
			int c = read();

			if (c < 128) // 0xxxxxxx
			{
				array[j++] = (char)c;
			}
			else if ((c & 0xE0) == 0xC0) // 110xxxxx
			{
				array[j++] = (char)(((c & 0x1F) << 6) | (read() & 0x3F));
			}
			else if ((c & 0xF0) == 0xE0) // 1110xxxx
			{
				array[j++] = (char)(((c & 0x0F) << 12) | ((read() & 0x3F) << 6) | (read() & 0x3F));
			}
			else
			{
				throw new RuntimeException("This decoder only handles 16-bit characters: c = " + c);
			}
		}

		return new String(array);
	}


	public ByteArrayBuffer writeString(String aInput)
	{
		align();
		ensureCapacity(aInput.length());

		for (int i = 0; i < aInput.length(); i++)
		{
			int c = aInput.charAt(i);

			if ((c >= 0x0000) && (c <= 0x007F))
			{
				write(c);
			}
			else if (c > 0x07FF)
			{
				write(0xE0 | ((c >> 12) & 0x0F));
				write(0x80 | ((c >> 6) & 0x3F));
				write(0x80 | ((c) & 0x3F));
			}
			else
			{
				write(0xC0 | ((c >> 6) & 0x1F));
				write(0x80 | ((c) & 0x3F));
			}
		}
		return this;
	}


	public int readBit()
	{
		if (mReadBitCount == 0)
		{
			mBitBuffer = read();

			mReadBitCount = 8;
		}

		mReadBitCount--;
		int output = 1 & (mBitBuffer >> mReadBitCount);
		mBitBuffer &= (1L << mReadBitCount) - 1;

		return output;
	}


	public ByteArrayBuffer writeBit(boolean aBit)
	{
		return writeBit(aBit ? 1 : 0);
	}


	public ByteArrayBuffer writeBit(int aBit)
	{
		mBitBuffer |= aBit << --mWriteBitsToGo;

		if (mWriteBitsToGo == 0)
		{
			ensureCapacity(1);
			mBuffer[mOffset++] = (byte)mBitBuffer;
			mBitBuffer = 0;
			mWriteBitsToGo = 8;
		}

		return this;
	}


	public int readBits(int aCount) throws IOException
	{
		assert aCount <= 24;

		int output = 0;

		while (aCount > mReadBitCount)
		{
			aCount -= mReadBitCount;
			output |= mBitBuffer << aCount;
			mBitBuffer = read();
			mReadBitCount = 8;

			if (mBitBuffer == -1)
			{
				mReadBitCount = 0;
				throw new IOException("Premature end of stream");
			}
		}

		if (aCount > 0)
		{
			mReadBitCount -= aCount;
			output |= mBitBuffer >> mReadBitCount;
			mBitBuffer &= (1 << mReadBitCount) - 1;
		}

		return output;
	}


	public ByteArrayBuffer writeBits(int aValue, int aLength)
	{
		while (aLength-- > 0)
		{
			writeBit((aValue >>> aLength) & 1);
		}

		return this;
	}


	public void align()
	{
		if (mWriteBitsToGo < 8)
		{
			ensureCapacity(1);
			mBuffer[mOffset] = (byte)mBitBuffer;
			mOffset++;
		}

		mBitBuffer = 0;
		mWriteBitsToGo = 8;
		mReadBitCount = 0;
	}


	private void flushBits()
	{
		if (mWriteBitsToGo != 8)
		{
			ensureCapacity(1);
			mBuffer[mOffset] = (byte)mBitBuffer;
		}
	}


	private static int encodeZigZag32(final int n)
	{
		return (n << 1) ^ (n >> 31);
	}


	private static int decodeZigZag32(final int n)
	{
		return (n >>> 1) ^ -(n & 1);
	}


	private static long encodeZigZag64(final long n)
	{
		return (n << 1) ^ (n >> 63);
	}


	private static long decodeZigZag64(final long n)
	{
		return (n >>> 1) ^ -(n & 1);
	}


	public static short readInt16(byte[] aBuffer, int aOffset)
	{
		int ch1 = 0xFF & aBuffer[aOffset];
		int ch2 = 0xFF & aBuffer[aOffset + 1];
		return (short)((ch1 << 8) + ch2);
	}


	public static void writeInt16(byte[] aBuffer, int aOffset, short aValue)
	{
		aBuffer[aOffset++] = (byte)(aValue >> 8);
		aBuffer[aOffset  ] = (byte)(aValue);
	}


	public static int readInt32(byte[] aBuffer, int aOffset)
	{
		int ch1 = 0xFF & aBuffer[aOffset++];
		int ch2 = 0xFF & aBuffer[aOffset++];
		int ch3 = 0xFF & aBuffer[aOffset++];
		int ch4 = 0xFF & aBuffer[aOffset];
		return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + ch4);
	}


	public static void writeInt32(byte[] aBuffer, int aOffset, int aValue)
	{
		aBuffer[aOffset++] = (byte)(aValue >>> 24);
		aBuffer[aOffset++] = (byte)(aValue >> 16);
		aBuffer[aOffset++] = (byte)(aValue >> 8);
		aBuffer[aOffset  ] = (byte)(aValue);
	}


	public static long readInt64(byte[] aBuffer, int aOffset)
	{
		return (((long)aBuffer[aOffset++] << 56)
			+ ((long)(aBuffer[aOffset++] & 255) << 48)
			+ ((long)(aBuffer[aOffset++] & 255) << 40)
			+ ((long)(aBuffer[aOffset++] & 255) << 32)
			+ ((long)(aBuffer[aOffset++] & 255) << 24)
			+ ((aBuffer[aOffset++] & 255) << 16)
			+ ((aBuffer[aOffset++] & 255) << 8)
			+ (aBuffer[aOffset] & 255));
	}


	public static void writeInt64(byte[] aBuffer, int aOffset, long aValue)
	{
		aBuffer[aOffset++] = (byte)(aValue >>> 56);
		aBuffer[aOffset++] = (byte)(aValue >> 48);
		aBuffer[aOffset++] = (byte)(aValue >> 40);
		aBuffer[aOffset++] = (byte)(aValue >> 32);
		aBuffer[aOffset++] = (byte)(aValue >> 24);
		aBuffer[aOffset++] = (byte)(aValue >> 16);
		aBuffer[aOffset++] = (byte)(aValue >> 8);
		aBuffer[aOffset  ] = (byte)(aValue);
	}


	public static void writeVar32(OutputStream aOutputStream, int aValue) throws IOException
	{
		aValue = encodeZigZag32(aValue);

		while (true)
		{
			if ((aValue & ~127) == 0)
			{
				aOutputStream.write(aValue);
				break;
			}
			else
			{
				aOutputStream.write(128 | (aValue & 127));
				aValue >>>= 7;
			}
		}
	}
}
