package org.terifan.raccoon;


public final class BlockSizeParam implements OpenParam
{
	private int mValue;


	public BlockSizeParam(int aValue)
	{
		mValue = aValue;
	}


	public int getValue()
	{
		return mValue;
	}
}
