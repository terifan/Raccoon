package org.terifan.raccoon;

import org.terifan.raccoon.storage.BlockType;


public final class CompressionParam
{
	public final static int NONE = 0;
	public final static int ZLE = 1;
	public final static int DEFLATE_FAST = 2;
	public final static int DEFLATE_DEFAULT = 3;
	public final static int DEFLATE_BEST = 4;

	public final static CompressionParam BEST_SPEED = new CompressionParam(DEFLATE_FAST, ZLE, NONE);
	public final static CompressionParam BEST_COMPRESSION = new CompressionParam(DEFLATE_BEST, DEFLATE_BEST, DEFLATE_BEST);
	public final static CompressionParam NO_COMPRESSION = new CompressionParam(NONE, NONE, NONE);

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


	public int getCompressorId(BlockType aType)
	{
		switch (aType)
		{
			case NODE_LEAF:
				return mLeaf;
			case NODE_INDEX:
			case BLOB_INDEX:
				return mNode;
			case BLOB_DATA:
				return mBlob;
			default:
				return NONE;
		}
	}
}
