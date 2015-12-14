package org.terifan.raccoon;

import org.terifan.raccoon.util.ByteBufferMap;
import static org.terifan.raccoon.io.BlockPointer.Types.*;


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
	public int getType()
	{
		return NODE_LEAF;
	}
}