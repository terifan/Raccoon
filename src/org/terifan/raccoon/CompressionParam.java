package org.terifan.raccoon;

import org.terifan.bundle.Document;


public final class CompressionParam implements OpenParam
{
	public final static byte NONE = 0;
	public final static byte ZLE = 1;
	public final static byte DEFLATE_FAST = 2;
	public final static byte DEFLATE_DEFAULT = 3;
	public final static byte DEFLATE_BEST = 4;

	public final static CompressionParam BEST_SPEED = new CompressionParam(DEFLATE_FAST, ZLE, NONE);
	public final static CompressionParam BEST_COMPRESSION = new CompressionParam(DEFLATE_DEFAULT, DEFLATE_DEFAULT, DEFLATE_BEST);
	public final static CompressionParam NO_COMPRESSION = new CompressionParam(NONE, NONE, NONE);

	private Document mConf;


	public CompressionParam()
	{
		this(NONE, NONE, NONE);
	}


	public CompressionParam(byte aNode, byte aLeaf, byte aBlob)
	{
		mConf = new Document()
			.putNumber("leaf", aLeaf)
			.putNumber("node", aNode)
			.putNumber("blob", aBlob);
	}


	public byte getCompressorId(BlockType aType)
	{
		switch (aType)
		{
			case LEAF:
				return mConf.getByte("leaf");
			case INDEX:
			case BLOB_INDEX:
				return mConf.getByte("node");
			case BLOB_DATA:
				return mConf.getByte("blob");
			default:
				return NONE;
		}
	}


	public Document marshal()
	{
		return mConf;
	}


	public CompressionParam unmarshal(Document aDocument)
	{
		mConf.replaceAll(aDocument);
		return this;
	}


	@Override
	public String toString()
	{
		return mConf.toString();
	}
}
