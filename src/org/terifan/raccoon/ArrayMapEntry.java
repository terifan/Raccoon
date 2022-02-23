package org.terifan.raccoon;

import java.util.Arrays;


public final class ArrayMapEntry
{
	private byte mType;
	private byte[] mKey;
	private byte[] mValue;


	public ArrayMapEntry()
	{
	}


	public ArrayMapEntry(byte[] aKey)
	{
		mKey = aKey;
	}


	public ArrayMapEntry(byte[] aKey, byte[] aValue, byte aType)
	{
		mKey = aKey;
		mValue = aValue;
		mType = aType;
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


	public byte getType()
	{
		return mType;
	}


	public void setType(byte aType)
	{
		mType = aType;
	}


	public void marshallValue(byte[] aBuffer, int aOffset)
	{
		aBuffer[aOffset] = mType;
		System.arraycopy(mValue, 0, aBuffer, aOffset + 1, mValue.length);
	}


	public void unmarshallValue(byte[] aBuffer, int aOffset, int aLength)
	{
		mType = aBuffer[aOffset];
		mValue = Arrays.copyOfRange(aBuffer, aOffset + 1, aOffset + aLength);
	}


	public int getMarshalledLength()
	{
		return 1 + mValue.length + mKey.length;
	}


	public int getMarshalledValueLength()
	{
		return 1 + mValue.length;
	}


	@Override
	public String toString()
	{
		return "ArrayMapEntry{" + "mType=" + mType + ", mKey=\"" + new String(mKey).replaceAll("[^\\w]*", "") + "\", mValue=\"" + new String(mValue).replaceAll("[^\\w]*", "") + "\"}";
	}
}
