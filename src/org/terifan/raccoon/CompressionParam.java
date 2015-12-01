package org.terifan.raccoon;


public class CompressionParam
{
	private int mNode;
	private int mLeaf;


	/**
	 * 
	 * @param aNode
	 *   compression used on node data. Using some compression is preferred.
	 * @param aLeaf 
	 *   compression used on leaf data ie the actual data records stored in the database. Using a fast or no compression at all is preferred.
	 */
	public CompressionParam(int aNode, int aLeaf)
	{
		mNode = aNode;
		mLeaf = aLeaf;
	}


	public int getLeaf()
	{
		return mLeaf;
	}


	public int getNode()
	{
		return mNode;
	}
}
