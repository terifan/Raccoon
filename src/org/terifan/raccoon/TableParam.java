package org.terifan.raccoon;


public final class TableParam
{
	public final static TableParam DEFAULT = new TableParam(4, 8, 1024);

	private int mPagesPerNode;
	private int mPagesPerLeaf;
	private int mBlockReadCacheSize;


	public TableParam(int aPagesPerNode, int aPagesPerLeaf, int aBlockReadCacheSize)
	{
		mPagesPerNode = aPagesPerNode;
		mPagesPerLeaf = aPagesPerLeaf;
		mBlockReadCacheSize = aBlockReadCacheSize;
	}


	public int getPagesPerNode()
	{
		return mPagesPerNode;
	}


	public int getPagesPerLeaf()
	{
		return mPagesPerLeaf;
	}


	public int getBlockReadCacheSize()
	{
		return mBlockReadCacheSize;
	}


	@Override
	public String toString()
	{
		return "TableParam{" + "mPagesPerNode=" + mPagesPerNode + ", mPagesPerLeaf=" + mPagesPerLeaf + ", mBlockReadCacheSize=" + mBlockReadCacheSize + '}';
	}
}
