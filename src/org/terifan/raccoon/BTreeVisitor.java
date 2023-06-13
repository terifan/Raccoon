package org.terifan.raccoon;

import org.terifan.raccoon.BTreeNode.VisitorState;


class BTreeVisitor
{
	VisitorState anyNode(BTree aImplementation, BTreeNode aNode)
	{
		return VisitorState.CONTINUE;
	}


	VisitorState beforeInteriorNode(BTree aImplementation, BTreeInteriorNode aNode, ArrayMapKey aLowestKey)
	{
		return VisitorState.CONTINUE;
	}


	VisitorState afterInteriorNode(BTree aImplementation, BTreeInteriorNode aNode)
	{
		return VisitorState.CONTINUE;
	}


	VisitorState leaf(BTree aImplementation, BTreeLeafNode aNode)
	{
		return VisitorState.CONTINUE;
	}
}