package org.terifan.raccoon;

import java.util.Iterator;
import java.util.LinkedList;


class BTreeNodeIterator implements Iterator<BTreeLeaf>
{
	private BTree mImplementation;
	private LinkedList<BTreeIndex> mIndexNodes;
	private LinkedList<BTreeLeaf> mLeafNodes;
	private ArrayMapKey mRangeLow;
	private ArrayMapKey mRangeHigh;


	BTreeNodeIterator(BTree aTree)
	{
		mImplementation = aTree;
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


	public void setRange(ArrayMapKey aLow, ArrayMapKey aHigh)
	{
		mRangeLow = aLow;
		mRangeHigh = aHigh;
	}


	@Override
	public boolean hasNext()
	{
		while (mLeafNodes.isEmpty() && !mIndexNodes.isEmpty())
		{
			BTreeIndex parent = mIndexNodes.remove(0);
			for (int i = 0, sz = parent.size(); i < sz; i++)
			{
				BTreeNode node = parent.getNode(mImplementation, i);
				if (node instanceof BTreeLeaf)
				{
					if ((mRangeLow == null || node.mMap.getLast().getKey().compareTo(mRangeLow) >= 0) && (mRangeHigh == null || node.mMap.getFirst().getKey().compareTo(mRangeHigh) <= 0))
					{
						mLeafNodes.add((BTreeLeaf)node);
					}
//					else
//						System.out.println("#" + parent.mMap+" "+node.mMap);
				}
				else
				{
//					if (i == 0)
//					{
//						if (parent.mMap.get(i, new ArrayMapEntry()).getKey().compareTo(high) <= 0)
//						{
//							mIndexNodes.add((BTreeIndex)node);
//						}
//					}
//					else
//					{
//						ArrayMapKey first = i == 0 ? parent.mMap.get(i, new ArrayMapEntry()).getKey() : node.mMap.get(i, new ArrayMapEntry()).getKey();
//						ArrayMapKey last = node.mMap.getLast().getKey();
//
//						System.out.println(node.mMap+" "+first.compareTo(high)+" "+last.compareTo(low));
//
//						if (first.compareTo(high) <= 0 && first.compareTo(low) >= 0)
//						if (high.compareTo(node.mMap.getLast().getKey()) >= 0)
//						{
//							System.out.println("*" + parent.mMap.get(i, new ArrayMapEntry()).getKey()+" -- "+parent.mMap.get(i+1, new ArrayMapEntry()).getKey()+" -- "+node.mMap);
//						}
//						else
//						{
							mIndexNodes.add((BTreeIndex)node);
//						}
//					}
				}
			}
		}

		return !mLeafNodes.isEmpty();
	}


	@Override
	public BTreeLeaf next()
	{
		return mLeafNodes.remove(0);
	}
}
