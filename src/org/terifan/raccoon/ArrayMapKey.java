package org.terifan.raccoon;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.UUID;
import org.terifan.raccoon.util.ByteArrayUtil;


public class ArrayMapKey implements Comparable<ArrayMapKey>
{
	public final static ArrayMapKey EMPTY = new ArrayMapKey(new byte[0]);

	private final byte[] mBuffer;
	private final int mFormat;


	public ArrayMapKey(String aValue)
	{
		mFormat = 0;
		mBuffer = aValue.getBytes(Charset.forName("utf-8"));
	}


	public ArrayMapKey(long aLongValue)
	{
		mFormat = 1;
		mBuffer = new byte[8];
		ByteArrayUtil.putInt64(mBuffer, 0, aLongValue);
	}


	public ArrayMapKey(byte[] aBuffer)
	{
		mFormat = 2;
		mBuffer = aBuffer.clone();
	}


	public ArrayMapKey(byte[] aBuffer, int aOffset, int aLength)
	{
		mFormat = 2;
		mBuffer = Arrays.copyOfRange(aBuffer, aOffset, aOffset + aLength);
	}


	public ArrayMapKey(UUID aUUID)
	{
		mFormat = 3;
		mBuffer = new byte[16];
		ByteArrayUtil.putInt64(mBuffer, 0, aUUID.getMostSignificantBits());
		ByteArrayUtil.putInt64(mBuffer, 8, aUUID.getLeastSignificantBits());
	}


	public byte[] array()
	{
		return mBuffer;
	}


	public int size()
	{
		return mBuffer.length;
	}


	@Override
	public int compareTo(ArrayMapKey aOther)
	{
		byte[] self = mBuffer;
		byte[] other = aOther.mBuffer;

		for (int i = 0, sz = Math.min(self.length, other.length); i < sz; i++)
		{
			int a = 0xff & self[i];
			int b = 0xff & other[i];
			if (a < b) return -1;
			if (a > b) return 1;
		}

		if (self.length < other.length) return -1;
		if (self.length > other.length) return 1;

		return 0;
	}


	@Override
	public int hashCode()
	{
		return Arrays.hashCode(mBuffer);
	}


	@Override
	public boolean equals(Object aOther)
	{
		if (aOther instanceof ArrayMapKey)
		{
			ArrayMapKey other = (ArrayMapKey)aOther;
			return Arrays.equals(mBuffer, other.mBuffer);
		}
		return false;
	}


	@Override
	public String toString()
	{
		switch (mFormat)
		{
			case 0:
				return new String(mBuffer);
			case 1:
				return "#" + ByteArrayUtil.getInt64(mBuffer, 0);
			case 2:
			default:
				StringBuilder sb = new StringBuilder("0x");
				for (byte b : mBuffer)
				{
					sb.append(String.format("%02x", 0xff & b));
				}
				return sb.toString();
			case 3:
				return UUID.nameUUIDFromBytes(mBuffer).toString();
		}
	}
}
