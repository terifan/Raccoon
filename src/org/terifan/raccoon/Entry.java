package org.terifan.raccoon;


public class Entry
{
	protected byte[] mKey;
	protected byte[] mValue;


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
}
