package org.terifan.raccoon.hashtable;

import org.terifan.raccoon.ArrayMap;
import org.terifan.raccoon.BlockType;
import org.terifan.raccoon.Node;


public class LeafNode extends ArrayMap implements Node
{
	public LeafNode(int aCapacity)
	{
		super(aCapacity);
	}


	public LeafNode(byte[] aBuffer)
	{
		super(aBuffer);
	}


	public LeafNode(byte[] aBuffer, int aOffset, int aCapacity)
	{
		super(aBuffer, aOffset, aCapacity);
	}


	@Override
	public BlockType getType()
	{
		return BlockType.LEAF;
	}
}
