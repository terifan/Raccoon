package org.terifan.raccoon;


class BTreeVisitor
{
	boolean beforeAnyNode(BTreeNode aNode)
	{
		return true;
	}


	boolean beforeLeafNode(BTreeLeafNode aNode)
	{
		return true;
	}


	boolean beforeInteriorNode(BTreeInteriorNode aNode, ArrayMapKey aLowestKey, ArrayMapKey aHighestKey)
	{
		return true;
	}


	boolean afterInteriorNode(BTreeInteriorNode aNode)
	{
		return true;
	}


	boolean leaf(BTreeLeafNode aNode)
	{
		return true;
	}
}