package org.terifan.raccoon;


public final class TableParam
{
	public final static TableParam DEFAULT = new TableParam(4, 8);

	private int mPagesPerNode;
	private int mPagesPerLeaf;


	public TableParam(int aPagesPerNode, int aPagesPerLeaf)
	{
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
