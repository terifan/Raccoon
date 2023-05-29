package org.terifan.raccoon;

import java.util.Arrays;
import java.util.UUID;
import org.terifan.raccoon.document.Array;


public class ArrayMapKey
{
	public final static ArrayMapKey EMPTY = new ArrayMapKey("");

	private byte[] mBuffer;
	private Object mDeserialized;


	public ArrayMapKey()
	{
	}


	public ArrayMapKey(Object aValue)
	{
		mDeserialized = aValue;
		mBuffer = Array.of(aValue).toByteArray();
	}


	public ArrayMapKey(byte[] aBuffer, int aOffset, int aLength)
	{
		mBuffer = Arrays.copyOfRange(aBuffer, aOffset, aOffset + aLength);
	}


	public byte[] array()
	{
		return mBuffer;
	}


	public int size()
	{
		return mBuffer.length;
	}


	public int compareTo(ArrayMapKey aOther)
	{
		Object a = get();
		Object b = aOther.get();

//		System.out.println(a + "\t" + b);

		if (a.getClass() != b.getClass() || a instanceof UUID)
		{
			return a.toString().compareTo(b.toString());
		}

		return ((Comparable)a).compareTo(b);
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
		return get().toString();
	}


	public Object get()
	{
		return mDeserialized = (mDeserialized != null ? mDeserialized : new Array().fromByteArray(mBuffer).get(0));
	}
}
