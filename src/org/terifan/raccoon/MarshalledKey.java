package org.terifan.raccoon;

import java.util.Arrays;


public class MarshalledKey implements Comparable<MarshalledKey>
{
	byte[] key;


	public MarshalledKey()
	{
	}


	public MarshalledKey(byte[] aKey)
	{
		key = aKey;
	}


	public byte[] getKey()
	{
		return key;
	}


	@Override
	public int compareTo(MarshalledKey aOther)
	{
		for (int i = 0; i < key.length; i++)
		{
//			int c = Integer.compareUnsigned(aOther.key[i], key[i]);
			int c = aOther.key[i] - key[i];
			if (c != 0)
			{
				return c;
			}
		}
		return 0;
	}


	@Override
	public int hashCode()
	{
		int hash = 7;
		hash = 97 * hash + Arrays.hashCode(this.key);
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
		if (!Arrays.equals(this.key, other.key))
		{
			return false;
		}
		return true;
	}
}
