package org.terifan.raccoon;


public class CompressionParam
{
	public final static CompressionParam BEST_SPEED = new CompressionParam(2, 1, 0);
	public final static CompressionParam BEST_COMPRESSION = new CompressionParam(9, 9, 9);
	public final static CompressionParam NO_COMPRESSION = new CompressionParam(0, 0, 0);

	private int mNode;
	private int mLeaf;
	private int mBlob;


	public CompressionParam(int aNode, int aLeaf, int aBlob)
	{
		mNode = aNode;
		mLeaf = aLeaf;
		mBlob = aBlob;
	}


	public int getLeaf()
	{
		return mLeaf;
	}


	public int getNode()
	{
		return mNode;
	}


	public int getBlob()
	{
		return mBlob;
	}


	@Override
	public String toString()
	{
		return "{node=" + mNode + ", leaf=" + mLeaf + ", blob=" + mBlob + "}";
	}
}
