package org.terifan.raccoon;

import java.util.Arrays;


public class MarshalledKey implements Comparable<MarshalledKey>
{
	private byte[] mBuffer;


	private MarshalledKey()
	{
	}


	public MarshalledKey(byte[] aBuffer)
	{
		mBuffer = aBuffer;
	}


	public byte[] array()
	{
		return mBuffer.clone();
	}


	public int size()
	{
		return mBuffer.length;
	}


	@Override
	public int compareTo(MarshalledKey aOther)
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
		if (aOther instanceof MarshalledKey other)
		{
			return Arrays.equals(this.mBuffer, other.mBuffer);
		}
		return false;
	}


	@Override
	public String toString()
	{
		return new String(mBuffer);
	}
}
