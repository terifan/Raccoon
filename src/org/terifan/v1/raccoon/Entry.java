package org.terifan.v1.raccoon;


public class Entry
{
	protected byte[] mKey;
	protected byte[] mValue;
	protected int mType;


	public Entry()
	{
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


	public int getType()
	{
		return mType;
	}


	public void setType(int aType)
	{
		mType = aType;
	}
}
