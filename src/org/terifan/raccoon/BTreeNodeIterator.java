package org.terifan.raccoon;

import java.util.Iterator;
import java.util.LinkedList;


class BTreeNodeIterator implements Iterator<BTreeLeaf>
{
	private BTree mImplementation;
	private LinkedList<BTreeNode> mList;
	private BTreeLeaf mPending;


	BTreeNodeIterator(BTree aTree)
	{
		mImplementation = aTree;
		mList = new LinkedList<>();
		mList.add(aTree.getRoot());
	}


	@Override
	public boolean hasNext()
	{
		if (mPending != null)
		{
			return true;
		}

		while (!mList.isEmpty() && mList.get(0) instanceof BTreeIndex)
		{
			BTreeIndex node = (BTreeIndex)mList.remove(0);
			for (int i = 0; i < node.size(); i++)
			{
				mList.add(i, node.getNode(mImplementation, i));
			}
		}

		if (mList.isEmpty())
		{
			return false;
		}

		mPending = (BTreeLeaf)mList.remove(0);

		return true;
	}


	@Override
	public BTreeLeaf next()
	{
		BTreeLeaf tmp = mPending;
		mPending = null;
		return tmp;
	}
}
