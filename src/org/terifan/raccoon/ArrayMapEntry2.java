package org.terifan.raccoon;

import java.util.Arrays;


public final class ArrayMapEntry2
{
	private byte mFlags;
	private byte[] mKey;
	private byte[] mValue;


	public ArrayMapEntry2()
	{
	}


	public ArrayMapEntry2(byte[] aKey)
	{
		mKey = aKey;
	}


	public ArrayMapEntry2(byte[] aKey, byte[] aValue, byte aFlags)
	{
		mKey = aKey;
		mValue = aValue;
		mFlags = aFlags;
	}


	public byte[] getKey()
	{
		return mKey;
	}


	public void setKey(byte[] aKey)
	{
		mKey = aKey;
	}


	public byte[] getValue()
	{
		return mValue;
	}


	public void setValue(byte[] aValue)
	{
		mValue = aValue;
	}


	public byte getFlags()
	{
		return mFlags;
	}


	public void setFlags(byte aFlags)
	{
		mFlags = aFlags;
	}


	public boolean hasFlag(byte aFlag)
	{
		return (mFlags & aFlag) == aFlag;
	}


	public void marshall(byte[] aBuffer, int aOffset)
	{
		aBuffer[aOffset] = mFlags;
		System.arraycopy(mValue, 0, aBuffer, aOffset + 1, mValue.length);
	}


	public void unmarshall(byte[] aBuffer, int aOffset, int aLength)
	{
		mFlags = aBuffer[aOffset];
		mValue = Arrays.copyOfRange(aBuffer, aOffset + 1, aOffset + aLength);
	}


	public int getMarshalledLength()
	{
		return 1 + mValue.length;
	}
}
