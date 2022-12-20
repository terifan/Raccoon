package org.terifan.raccoon;

import java.nio.charset.Charset;
import java.util.Arrays;


public class ArrayMapKey implements Comparable<ArrayMapKey>
{
	private final byte[] mBuffer;


	public ArrayMapKey(byte[] aBuffer)
	{
		mBuffer = aBuffer;
	}


	public ArrayMapKey(String aValue)
	{
		mBuffer = aValue.getBytes(Charset.forName("utf-8"));
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
		return new String(mBuffer);
	}
}
