package org.terifan.raccoon.btree;

import java.util.Arrays;
import java.util.Iterator;


// 0----0----0
//       ----1
//       ----2
//      1----0
//       ----1
//       ----2
// 1----0----0
//       ----1
//       ----2
//      1----0
//       ----1
//       ----2

public class BTreeNodeIterator implements Iterator<BTreeLeaf>
{
	private BTree mImplementation;
	private BTreeNode mRoot;
	private BTreeLeaf mPending;
	private int[] mCounters;


	BTreeNodeIterator(BTree aTree, BTreeNode aRoot)
	{
		mImplementation = aTree;
		mRoot = aRoot;
		mCounters = new int[10];

		if (aRoot instanceof BTreeLeaf)
		{
			mPending = (BTreeLeaf)aRoot;
			mRoot = null;
		}
	}

	@Override
	public boolean hasNext()
	{
		if (mPending != null)
		{
			return true;
		}
		if (mRoot == null)
		{
			return false;
		}

		int level = 0;
		BTreeNode node = mRoot;

		while (node instanceof BTreeIndex)
		{
			BTreeIndex indexNode = (BTreeIndex)node;

			if (level >= mCounters.length)
			{
				mCounters = Arrays.copyOfRange(mCounters, 0, level * 3 / 2 + 1);
			}

			if (mCounters[level] >= indexNode.size())
			{
				if (level == 0)
				{
					mImplementation = null;
					mRoot = null;
					return false;
				}
				mCounters[level - 1]++;
				mCounters[level] = 0;
				return hasNext();
			}

			node = indexNode.getNode(mImplementation, mCounters[level]);

			if (node instanceof BTreeLeaf) break;
			level++;
		}

		mCounters[level]++;

		mPending = (BTreeLeaf)node;

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
