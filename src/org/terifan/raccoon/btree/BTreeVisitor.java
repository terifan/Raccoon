package org.terifan.raccoon.btree;


public class BTreeVisitor
{
	public boolean beforeAnyNode(BTreeNode aNode)
	{
		return true;
	}


	public boolean beforeLeafNode(BTreeLeafNode aNode)
	{
		return true;
	}


	public boolean beforeInteriorNode(BTreeInteriorNode aNode, ArrayMapKey aLowestKey, ArrayMapKey aHighestKey)
	{
		return true;
	}


	public boolean afterInteriorNode(BTreeInteriorNode aNode)
	{
		return true;
	}


	public boolean leaf(BTreeLeafNode aNode)
	{
		return true;
	}
}