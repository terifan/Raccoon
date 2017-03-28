package org.terifan.raccoon.hashtable;


public final class LeafEntry
{
	byte mFlags;
	byte[] mKey;
	byte[] mValue;


	public LeafEntry()
	{
	}


	public LeafEntry(byte[] aKey)
	{
		mKey = aKey;
	}


	public LeafEntry(byte[] aKey, byte[] aValue, byte aFlags)
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
}
