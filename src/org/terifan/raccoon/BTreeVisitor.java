package org.terifan.raccoon;

import org.terifan.raccoon.BTreeNode.VisitorState;


class BTreeVisitor
{
	VisitorState anyNode(BTree aImplementation, BTreeNode aNode)
	{
		return VisitorState.CONTINUE;
	}


	VisitorState beforeIndex(BTree aImplementation, BTreeIndex aNode, ArrayMapKey aLowestKey)
	{
		return VisitorState.CONTINUE;
	}


	VisitorState afterIndex(BTree aImplementation, BTreeIndex aNode)
	{
		return VisitorState.CONTINUE;
	}


	VisitorState leaf(BTree aImplementation, BTreeLeaf aNode)
	{
		return VisitorState.CONTINUE;
	}
}