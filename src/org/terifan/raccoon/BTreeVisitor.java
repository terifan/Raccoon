package org.terifan.raccoon;

import org.terifan.raccoon.BTreeNode.VisitorState;


class BTreeVisitor
{
	boolean anyNode(BTree aImplementation, BTreeNode aNode)
	{
		return true;
	}


	boolean beforeLeafNode(BTree aImplementation, BTreeLeafNode aNode)
	{
		return true;
	}


	boolean beforeInteriorNode(BTree aImplementation, ArrayMapKey aLowestKey, ArrayMapKey aHighestKey)
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