package org.terifan.raccoon;


public class LeafNode extends ByteBufferMap implements Node
{
	private LeafNode(byte[] aBuffer)
	{
		super(aBuffer);
	}


	private LeafNode(int aCapacity)
	{
		super(aCapacity);
	}


	public static LeafNode alloc(int aCapacity)
	{
		Stats.leafNodeCreation++;

		return new LeafNode(aCapacity);
	}


	public static LeafNode wrap(byte[] aBuffer)
	{
		Stats.leafNodeCreation++;

		return new LeafNode(aBuffer);
	}


	@Override
	public int getType()
	{
		return Node.LEAF;
	}
}