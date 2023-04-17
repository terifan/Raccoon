package org.terifan.raccoon;

import java.util.Arrays;
import org.terifan.raccoon.io.util.Console;


public final class ArrayMapEntry
{
	private byte mType;
	private ArrayMapKey mKey;
	private byte[] mValue;


	public ArrayMapEntry()
	{
	}


	public ArrayMapEntry(ArrayMapKey aKey)
	{
		mKey = aKey;
	}


	public ArrayMapEntry(ArrayMapKey aKey, byte[] aValue, byte aType)
	{
		mKey = aKey;
		mValue = aValue;
		mType = aType;
	}


	public ArrayMapKey getKey()
	{
		return mKey;
	}


	public void setKey(ArrayMapKey aKey)
	{
		mKey = aKey;
	}


	public void unmarshallKey(byte[] aBuffer, int aOffset, int aLength)
	{
		setKey(new ArrayMapKey(aBuffer, aOffset, aLength));
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
		return 1 + mValue.length + mKey.size();
	}


	public int getMarshalledValueLength()
	{
		return 1 + mValue.length;
	}


	@Override
	public String toString()
	{
		return Console.format("ArrayMapEntry{mType=%s, mKey=%s, mValue=%s}", mType, (mKey == null ? "null" : "\"" + new String(mKey.array()).replaceAll("[^\\w]*", "") + "\""), (mValue == null ? "null" : "\"" + new String(mValue).replace('\u0000', '.').replaceAll("[^\\w\\.]*", "") + "\""));
	}
}
