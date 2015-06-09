package org.terifan.raccoon;


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
		return Node.LEAF;
	}
}