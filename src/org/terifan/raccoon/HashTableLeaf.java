package org.terifan.raccoon;


class HashTableLeaf extends ArrayMap implements Node
{
	public HashTableLeaf(int aCapacity)
	{
		super(aCapacity);
	}


	public HashTableLeaf(byte[] aBuffer)
	{
		super(aBuffer);
	}


	public HashTableLeaf(byte[] aBuffer, int aOffset, int aCapacity)
	{
		super(aBuffer, aOffset, aCapacity);
	}


	@Override
	public BlockType getType()
	{
		return BlockType.LEAF;
	}
}
