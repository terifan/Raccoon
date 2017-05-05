package org.terifan.raccoon.btree;

import org.terifan.raccoon.core.Node;
import org.terifan.raccoon.core.BlockType;


final class IndexNode implements Node
{
	@Override
	public byte[] array()
	{
		return null;
	}


	@Override
	public BlockType getType()
	{
		return BlockType.INDEX;
	}
}
