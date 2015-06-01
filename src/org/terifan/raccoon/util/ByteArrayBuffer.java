package org.terifan.raccoon.util;

import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;


public class ByteArrayBuffer
{
	private final static boolean FORCE_FIXED = false;

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
		mLimit = Integer.MAX_VALUE;
		mBitBuffer = 0;
		mWriteBitsToGo = 8;
	}


	/**
	 * Read/write from a fixed size buffer. Writing beyond the buffer will cause an exception.
	 */
	public ByteArrayBuffer(byte[] aBuffer)
	{
		mBuffer = aBuffer;
		mLocked = true;
		mLimit = Integer.MAX_VALUE;
		mBitBuffer = 0;
		mWriteBitsToGo = 8;
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


	public ByteArrayBuffer position(int aOffset) throws IOException
	{
		align();
		mOffset = aOffset;
		return this;
	}


	public int position()
	{
		return mOffset;
	}


	public ByteArrayBuffer skip(int aLength) throws IOException
	{
		flushBits();
		mOffset += aLength;
		return this;
	}


	private ByteArrayBuffer ensureCapacity(int aIncrement) throws IOException
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
		mBuffer = Arrays.copyOfRange(mBuffer, 0, mOffset);
		return this;
	}


	public ByteArrayBuffer trim(int aNewLength)
	{
		mBuffer = Arrays.copyOfRange(mBuffer, 0, aNewLength);
		mOffset = Math.min(mOffset, aNewLength);
		return this;
	}


	public byte[] array() throws IOException
	{
		flushBits();

		return mBuffer;
	}


	public int read() throws IOException
	{
		if (mOffset >= mBuffer.length || mOffset >= mLimit)
		{
			throw new EOFException("Reading beyond end of buffer, capacity " + mBuffer.length + ", offset " + mOffset + ", limit " + mLimit);
		}

		align();
		
		return 0xff & mBuffer[mOffset++];
	}


	public ByteArrayBuffer write(int aByte) throws IOException
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
			if ((b & 128) == 0)
			{
				return decodeZigZag32(value);
			}
		}

		throw new IOException("Variable int exceeds maximum length");
	}


	public ByteArrayBuffer writeVar32(int aValue) throws IOException
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

		long value = 0L;
		for (int n = 0; n < 64; n += 7)
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


	public ByteArrayBuffer writeVar64(long aValue) throws IOException
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


	public byte[] read(byte[] aBuffer) throws IOException
	{
		return read(aBuffer, 0, aBuffer.length);
	}


	public byte[] read(byte[] aBuffer, int aOffset, int aLength) throws IOException
	{
		if (mOffset + aLength > mBuffer.length || mOffset + aLength >= mLimit)
		{
			throw new EOFException("Reading beyond end of buffer, capacity " + mBuffer.length + ", offset " + mOffset + ", limit " + mLimit);
		}

		align();
		System.arraycopy(mBuffer, mOffset, aBuffer, aOffset, aLength);
		mOffset += aLength;
		return aBuffer;
	}


	public ByteArrayBuffer write(byte[] aBuffer) throws IOException
	{
		return write(aBuffer, 0, aBuffer.length);
	}


	public ByteArrayBuffer write(byte[] aBuffer, int aOffset, int aLength) throws IOException
	{
		align();
		ensureCapacity(aLength);

		System.arraycopy(aBuffer, aOffset, mBuffer, mOffset, aLength);
		mOffset += aLength;
		return this;
	}


	public int readInt32() throws IOException
	{
		align();
		int ch1 = read();
		int ch2 = read();
		int ch3 = read();
		int ch4 = read();
		return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + ch4);
	}


	public ByteArrayBuffer writeInt32(int aValue) throws IOException
	{
		align();
		ensureCapacity(4);
		mBuffer[mOffset++] = (byte)(aValue >>> 24);
		mBuffer[mOffset++] = (byte)(aValue >> 16);
		mBuffer[mOffset++] = (byte)(aValue >> 8);
		mBuffer[mOffset++] = (byte)(aValue);
		return this;
	}


	public long readInt64() throws IOException
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


	public ByteArrayBuffer writeInt64(long aValue) throws IOException
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


	public float readFloat() throws IOException
	{
		return Float.intBitsToFloat(readInt32());
	}


	public ByteArrayBuffer writeFloat(float aFloat) throws IOException
	{
		return writeInt32(Float.floatToIntBits(aFloat));
	}


	public double readDouble() throws IOException
	{
		return Double.longBitsToDouble(readInt64());
	}


	public ByteArrayBuffer writeDouble(double aDouble) throws IOException
	{
		return writeInt64(Double.doubleToLongBits(aDouble));
	}


	public String readString(int aLength) throws IOException
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


	public ByteArrayBuffer writeString(String aInput) throws IOException
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


	public int readBit() throws IOException
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


	public ByteArrayBuffer writeBit(int aBit) throws IOException
	{
		mBitBuffer |= aBit << --mWriteBitsToGo;

		if (mWriteBitsToGo == 0)
		{
			mBuffer[mOffset++] = (byte)mBitBuffer;
			mBitBuffer = 0;
			mWriteBitsToGo = 8;
		}
		
		return this;
	}


	public int readBits(int aCount) throws IOException
	{
		int output = 0;

		while (aCount > mReadBitCount)
		{
			aCount -= mReadBitCount;
			output |= mBitBuffer << aCount;
			mBitBuffer = read();
			mReadBitCount = 8;
		}

		if (aCount > 0)
		{
			mReadBitCount -= aCount;
			output |= mBitBuffer >> mReadBitCount;
			mBitBuffer &= (1L << mReadBitCount) - 1;
		}

		return output;
	}

	
	public ByteArrayBuffer writeBits(int aValue, int aLength) throws IOException
	{
		while (aLength-- > 0)
		{
			writeBit((aValue >>> aLength) & 1);
		}
		
		return this;
	}

	
	private void align() throws IOException
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


	private void flushBits() throws IOException
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


	private static long decodeZigZag64(final long n)
	{
		return (n >>> 1) ^ -(n & 1);
	}


	private static long encodeZigZag64(final long n)
	{
		return (n << 1) ^ (n >> 63);
	}


	private static int decodeZigZag32(final int n)
	{
		return (n >>> 1) ^ -(n & 1);
	}
}
