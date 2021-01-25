package org.terifan.raccoon;

import org.terifan.raccoon.util.ByteArrayBuffer;


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

	private byte mNode;
	private byte mLeaf;
	private byte mBlob;


	public CompressionParam()
	{
		this(NONE, NONE, NONE);
	}


	public CompressionParam(byte aNode, byte aLeaf, byte aBlob)
	{
		mNode = aNode;
		mLeaf = aLeaf;
		mBlob = aBlob;
	}


	public byte getLeaf()
	{
		return mLeaf;
	}


	public byte getNode()
	{
		return mNode;
	}


	public byte getBlob()
	{
		return mBlob;
	}


	@Override
	public String toString()
	{
		return "{node=" + mNode + ", leaf=" + mLeaf + ", blob=" + mBlob + "}";
	}


	public byte getCompressorId(BlockType aType)
	{
		switch (aType)
		{
			case LEAF:
				return mLeaf;
			case INDEX:
			case BLOB_INDEX:
				return mNode;
			case BLOB_DATA:
				return mBlob;
			default:
				return NONE;
		}
	}


	public void marshal(ByteArrayBuffer aBuffer)
	{
		aBuffer.writeInt8(mLeaf);
		aBuffer.writeInt8(mNode);
		aBuffer.writeInt8(mBlob);
	}


	public void unmarshal(ByteArrayBuffer aBuffer)
	{
		mLeaf = (byte)aBuffer.readInt8();
		mNode = (byte)aBuffer.readInt8();
		mBlob = (byte)aBuffer.readInt8();
	}
}
