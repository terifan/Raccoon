package org.terifan.raccoon;


class BTreeVisitor
{
	boolean beforeAnyNode(BTree aImplementation, BTreeNode aNode)
	{
		return true;
	}


	boolean beforeLeafNode(BTree aImplementation, BTreeLeafNode aNode)
	{
		return true;
	}


	boolean beforeInteriorNode(BTree aImplementation, BTreeInteriorNode aNode, ArrayMapKey aLowestKey, ArrayMapKey aHighestKey)
	{
		return true;
	}


	boolean afterInteriorNode(BTree aImplementation, BTreeInteriorNode aNode)
	{
		return true;
	}


	boolean leaf(BTree aImplementation, BTreeLeafNode aNode)
	{
		return true;
	}
}