package org.terifan.raccoon;

import org.terifan.raccoon.io.BlockPointer.BlockType;


public class LeafNode extends ByteBufferMap implements Node
{
	public LeafNode(byte[] aBuffer)
	{
		super(aBuffer);
		Stats.leafNodeCreation.incrementAndGet();
	}


	public LeafNode(int aCapacity)
	{
		super(aCapacity);
		Stats.leafNodeCreation.incrementAndGet();
	}


	@Override
	public BlockType getType()
	{
		return BlockType.NODE_LEAF;
	}
}