package org.terifan.raccoon;

import org.terifan.raccoon.io.BlockPointer.BlockType;
import org.terifan.raccoon.util.ByteBufferMap;


public class LeafNode extends ByteBufferMap implements Node
{
	public LeafNode(byte[] aBuffer)
	{
		super(aBuffer);
		Stats.leafNodeCreation++;
	}


	public LeafNode(int aCapacity)
	{
		super(aCapacity);
		Stats.leafNodeCreation++;
	}


	@Override
	public BlockType getType()
	{
		return BlockType.NODE_LEAF;
	}
}