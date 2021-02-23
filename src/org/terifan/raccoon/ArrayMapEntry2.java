package org.terifan.raccoon;

import java.util.Arrays;


public final class ArrayMapEntry2
{
	private byte[] mKey;
	private byte[] mValue;


	public ArrayMapEntry2()
	{
	}


	public ArrayMapEntry2(byte[] aKey)
	{
		mKey = aKey;
	}


	public ArrayMapEntry2(byte[] aKey, byte[] aValue)
	{
		mKey = aKey;
		mValue = aValue;
	}


	public byte[] getKey()
	{
		return mKey;
	}


	public void setKey(byte[] aKey)
	{
		mKey = aKey;
	}


	public byte[] getValue(byte[] aBuffer, int aOffset)
	{
		System.arraycopy(mValue, 0, aBuffer, aOffset, mValue.length);
		return aBuffer;
	}


	public void setValue(byte[] aBuffer, int aOffset, int aLength)
	{
		mValue = Arrays.copyOfRange(aBuffer, aOffset, aOffset + aLength);
	}


	public int getValueLength()
	{
		return mValue.length;
	}
}
