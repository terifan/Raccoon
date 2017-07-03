package org.terifan.raccoon.btree;

import org.terifan.raccoon.core.ArrayMap;
import org.terifan.raccoon.core.BlockType;
import org.terifan.raccoon.core.Node;


public class LeafNode extends ArrayMap implements Node
{
	public LeafNode(int aCapacity)
	{
		super(aCapacity);
	}


	@Override
	public BlockType getType()
	{
		return BlockType.LEAF;
	}
}
