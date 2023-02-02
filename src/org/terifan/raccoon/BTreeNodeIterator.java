package org.terifan.raccoon;

import java.util.LinkedList;


class BTreeNodeIterator extends Sequence<BTreeLeaf>
{
	private BTree mImplementation;
	private LinkedList<BTreeIndex> mIndexNodes;
	private LinkedList<BTreeLeaf> mLeafNodes;
	private Query mQuery;


	BTreeNodeIterator(BTree aTree, Query aQuery)
	{
		mImplementation = aTree;
		mQuery = aQuery;

		mIndexNodes = new LinkedList<>();
		mLeafNodes = new LinkedList<>();

		BTreeNode root = aTree.getRoot();
		if (root instanceof BTreeIndex)
		{
			mIndexNodes.add((BTreeIndex)root);
		}
		else
		{
			mLeafNodes.add((BTreeLeaf)root);
		}
	}


	@Override
	public BTreeLeaf advance()
	{
		while (mLeafNodes.isEmpty() && !mIndexNodes.isEmpty())
		{
			BTreeIndex parent = mIndexNodes.remove(0);

			for (int i = 0; i < parent.size(); i++)
			{
				BTreeNode node = parent.getNode(mImplementation, i);

				if (node instanceof BTreeLeaf)
				{
					if ((mQuery.mRangeLow == null || node.mMap.getLast().getKey().compareTo(mQuery.mRangeLow) >= 0) && (mQuery.mRangeHigh == null || node.mMap.getFirst().getKey().compareTo(mQuery.mRangeHigh) <= 0))
					{
						mLeafNodes.add((BTreeLeaf)node);
					}
				}
				else
				{
					mIndexNodes.add((BTreeIndex)node);
				}
			}
		}

		if (mLeafNodes.isEmpty())
		{
			return null;
		}

		return mLeafNodes.remove(0);
	}
}
