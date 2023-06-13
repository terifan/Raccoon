package org.terifan.raccoon;

import java.util.Arrays;
import java.util.UUID;
import org.terifan.raccoon.document.Array;


public class ArrayMapKey
{
	public final static ArrayMapKey EMPTY = new ArrayMapKey("");

	private byte[] mSerialized;
	private Object mInstance;


	public ArrayMapKey()
	{
	}


	public ArrayMapKey(Object aInstance)
	{
		mInstance = aInstance;
		mSerialized = Array.of(aInstance).toByteArray();
	}


	public ArrayMapKey(byte[] aBuffer, int aOffset, int aLength)
	{
		mSerialized = Arrays.copyOfRange(aBuffer, aOffset, aOffset + aLength);
	}


	public byte[] array()
	{
		return mSerialized;
	}


	public int size()
	{
		return mSerialized.length;
	}


	public int compareTo(ArrayMapKey aOther)
	{
		Object a = get();
		Object b = aOther.get();

//		System.out.println(a + "\t" + b);

		if (a.getClass() != b.getClass())
		{
			return a.toString().compareTo(b.toString());
		}

		return ((Comparable)a).compareTo(b);
	}


	@Override
	public int hashCode()
	{
		return Arrays.hashCode(mSerialized);
	}


	@Override
	public boolean equals(Object aOther)
	{
		if (aOther instanceof ArrayMapKey)
		{
			ArrayMapKey other = (ArrayMapKey)aOther;
			return Arrays.equals(mSerialized, other.mSerialized);
		}
		return false;
	}


	@Override
	public String toString()
	{
		return get().toString();
	}


	public Object get()
	{
		return mInstance = (mInstance != null ? mInstance : new Array().fromByteArray(mSerialized).get(0));
	}
}
