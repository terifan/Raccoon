package org.terifan.raccoon.core;


public final class RecordEntry
{
	private byte mFlags;
	private byte[] mKey;
	private byte[] mValue;


	public RecordEntry()
	{
	}


	public RecordEntry(byte[] aKey)
	{
		mKey = aKey;
	}


	public RecordEntry(byte[] aKey, byte[] aValue, byte aFlags)
	{
		mKey = aKey;
		mValue = aValue;
		mFlags = aFlags;
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


	public int size()
	{
		return mKey.length + mValue.length + 1;
	}
}
