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


	public byte[] marshall()
	{
		return mBuffer.clone();
	}


	public int size()
	{
		return mBuffer.length;
	}


	public static MarshalledKey unmarshall(byte[] aBuffer)
	{
		MarshalledKey k = new MarshalledKey();
		k.mBuffer = aBuffer.clone();
		return k;
	}


	@Override
	public int compareTo(MarshalledKey aOther)
	{
		byte[] other = aOther.mBuffer;
		byte[] self = mBuffer;

		for (int i = 0, len = Math.min(self.length, other.length); i < len; i++)
		{
			int a = 0xff & self[i];
			int b = 0xff & other[i];
			if (a < b) return -1;
			if (a > b) return 1;
		}

		if (self.length > other.length) return 1;
		if (self.length < other.length) return -1;

		return 0;
	}


	@Override
	public int hashCode()
	{
		int hash = 7;
		hash = 97 * hash + Arrays.hashCode(mBuffer);
		return hash;
	}


	@Override
	public boolean equals(Object obj)
	{
		if (this == obj)
		{
			return true;
		}
		if (obj == null)
		{
			return false;
		}
		if (getClass() != obj.getClass())
		{
			return false;
		}
		final MarshalledKey other = (MarshalledKey)obj;
		if (!Arrays.equals(this.mBuffer, other.mBuffer))
		{
			return false;
		}
		return true;
	}


	@Override
	public String toString()
	{
		return new String(mBuffer);
	}
}
