package org.terifan.raccoon;


public final class TableParam implements OpenParam
{
	public final static int DEFAULT_PAGES_PER_NODE = 2;
	public final static int DEFAULT_PAGES_PER_LEAF = 4;
	public final static TableParam DEFAULT = new TableParam(DEFAULT_PAGES_PER_NODE, DEFAULT_PAGES_PER_LEAF);

	private int mPagesPerNode;
	private int mPagesPerLeaf;


	public TableParam(int aPagesPerNode, int aPagesPerLeaf)
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
	}


	public int getPagesPerNode()
	{
		return mPagesPerNode;
	}


	public int getPagesPerLeaf()
	{
		return mPagesPerLeaf;
	}


	@Override
	public String toString()
	{
		return "TableParam{" + "mPagesPerNode=" + mPagesPerNode + ", mPagesPerLeaf=" + mPagesPerLeaf + '}';
	}
}
