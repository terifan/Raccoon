package org.terifan.raccoon;

import org.terifan.bundle.Document;
import static org.terifan.raccoon.CompressionParam.Level.DEFLATE_BEST;
import static org.terifan.raccoon.CompressionParam.Level.DEFLATE_DEFAULT;
import static org.terifan.raccoon.CompressionParam.Level.DEFLATE_FAST;
import static org.terifan.raccoon.CompressionParam.Level.NONE;
import static org.terifan.raccoon.CompressionParam.Level.ZLE;


public final class CompressionParam implements OpenParam
{
	public enum Level
	{
		NONE,
		ZLE,
		DEFLATE_FAST,
		DEFLATE_DEFAULT,
		DEFLATE_BEST
	}

	public final static CompressionParam BEST_SPEED = new CompressionParam(DEFLATE_FAST, ZLE, ZLE, NONE);
	public final static CompressionParam BEST_COMPRESSION = new CompressionParam(DEFLATE_DEFAULT, DEFLATE_DEFAULT, DEFLATE_DEFAULT, DEFLATE_BEST);
	public final static CompressionParam NO_COMPRESSION = new CompressionParam(NONE, NONE, NONE, NONE);

	private Document mConfiguration;


	public CompressionParam()
	{
		this(NONE, NONE, NONE, NONE);
	}


	public CompressionParam(Level aTreeIndex, Level aTreeLeaf, Level aBlobIndex, Level aBlobLeaf)
	{
		mConfiguration = new Document()
			.putNumber("treeLeaf", aTreeLeaf.ordinal())
			.putNumber("treeIndex", aTreeIndex.ordinal())
			.putNumber("blobLeaf", aBlobLeaf.ordinal())
			.putNumber("blobIndex", aBlobIndex.ordinal());
	}


	public Level getCompressorLevel(BlockType aType)
	{
		switch (aType)
		{
			case TREE_INDEX:
				return Level.values()[mConfiguration.getInt("treeIndex", NONE.ordinal())];
			case TREE_LEAF:
				return Level.values()[mConfiguration.getInt("treeLeaf", NONE.ordinal())];
			case BLOB_INDEX:
				return Level.values()[mConfiguration.getInt("blobIndex", NONE.ordinal())];
			case BLOB_LEAF:
				return Level.values()[mConfiguration.getInt("blobNode", NONE.ordinal())];
			default:
				return NONE;
		}
	}


	public Document marshal()
	{
		return mConfiguration;
	}


	public static CompressionParam unmarshal(Document aDocument)
	{
		CompressionParam cp = new CompressionParam();
		cp.mConfiguration = aDocument;
		return cp;
	}


	@Override
	public String toString()
	{
		return mConfiguration.toString();
	}
}
