package org.terifan.raccoon;

import java.util.Arrays;


public final class ArrayMapEntry
{
	private byte mFlags;
	private byte[] mKey;
	private byte[] mValue;


	public ArrayMapEntry()
	{
	}


	public ArrayMapEntry(byte[] aKey)
	{
		mKey = aKey;
	}


	public ArrayMapEntry(byte[] aKey, byte[] aValue, byte aFlags)
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


	public void unmarshallKey(byte[] aBuffer, int aOffset, int aLength)
	{
		setKey(Arrays.copyOfRange(aBuffer, aOffset, aOffset + aLength));
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


	public void marshallValue(byte[] aBuffer, int aOffset)
	{
		aBuffer[aOffset] = mFlags;
		System.arraycopy(mValue, 0, aBuffer, aOffset + 1, mValue.length);
	}


	public void unmarshallValue(byte[] aBuffer, int aOffset, int aLength)
	{
		mFlags = aBuffer[aOffset];
		mValue = Arrays.copyOfRange(aBuffer, aOffset + 1, aOffset + aLength);
	}


	public int getMarshalledValueLength()
	{
		return 1 + mValue.length;
	}
}
