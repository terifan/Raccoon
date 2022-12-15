package org.terifan.raccoon;

import java.util.LinkedList;


class BTreeNodeVisitor
{
	public void visitAll(BTree aTree, Visitor aVisitor)
	{
		visit(aTree, aVisitor, 0);
	}


	public void visitLeafs(BTree aTree, Visitor aVisitor)
	{
		visit(aTree, aVisitor, 1);
	}


	public void visitIndices(BTree aTree, Visitor aVisitor)
	{
		visit(aTree, aVisitor, 2);
	}


	private Object visit(BTree aTree, Visitor aVisitor, int aMode)
	{
		try
		{
			LinkedList<BTreeNode> list = new LinkedList<>();
			list.add(aTree.getRoot());

			while (!list.isEmpty())
			{
				BTreeNode node = list.remove(0);

				if (node instanceof BTreeIndex)
				{
					BTreeIndex indexNode = (BTreeIndex)node;

					for (int i = 0; i < indexNode.size(); i++)
					{
						list.add(i, indexNode.getNode(aTree, i));
					}

					if (aMode != 1)
					{
						aVisitor.visit(node);
					}
				}
				else if (aMode != 2)
				{
					aVisitor.visit(node);
				}
			}
		}
		catch (CancelVisitor e)
		{
			return e.getResult();
		}

		return null;
	}


	public static class CancelVisitor extends RuntimeException
	{
		private final Object mResult;


		public CancelVisitor(Object aResult)
		{
			mResult = aResult;
		}


		public Object getResult()
		{
			return mResult;
		}
	}


	public static interface Visitor
	{
		void visit(BTreeNode aNode);
	}
}
