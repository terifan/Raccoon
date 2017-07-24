package org.terifan.raccoon;


public final class TableParam implements OpenParam
{
	public final static int DEFAULT_PAGES_PER_NODE = 2;
	public final static int DEFAULT_PAGES_PER_LEAF = 4;
	public final static int DEFAULT_READ_CACHE_SIZE = 1024;
	public final static TableParam DEFAULT = new TableParam(DEFAULT_PAGES_PER_NODE, DEFAULT_PAGES_PER_LEAF, DEFAULT_READ_CACHE_SIZE);

	private int mPagesPerNode;
	private int mPagesPerLeaf;
	private int mBlockReadCacheSize;


	public TableParam(int aPagesPerNode, int aPagesPerLeaf, int aBlockReadCacheSize)
	{
		if ((aPagesPerNode & -aPagesPerNode) != aPagesPerNode)
		{
			throw new IllegalArgumentException("Illegal aPagesPerNode");
		}
		if ((aPagesPerLeaf & -aPagesPerLeaf) != aPagesPerLeaf)
		{
			throw new IllegalArgumentException("Illegal aPagesPerLeaf");
		}

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
