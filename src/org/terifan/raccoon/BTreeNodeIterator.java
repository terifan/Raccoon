package org.terifan.raccoon;

import java.util.Iterator;
import java.util.LinkedList;


class BTreeNodeIterator implements Iterator<BTreeLeaf>
{
	private BTree mImplementation;
	private LinkedList<BTreeIndex> mIndexNodes;
	private LinkedList<BTreeLeaf> mLeafNodes;


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


	@Override
	public boolean hasNext()
	{
		ArrayMapKey low = new ArrayMapKey("35");
		ArrayMapKey high = new ArrayMapKey("52");

		while (mLeafNodes.isEmpty() && !mIndexNodes.isEmpty())
		{
			BTreeIndex parent = mIndexNodes.remove(0);
			for (int i = 0, sz = parent.size(); i < sz; i++)
			{
				BTreeNode node = parent.getNode(mImplementation, i);
				if (node instanceof BTreeLeaf)
				{
					BTreeLeaf tmp = (BTreeLeaf)node;
					if (tmp.mMap.getLast().getKey().compareTo(low) >= 0 && tmp.mMap.getFirst().getKey().compareTo(high) <= 0)
					{
						mLeafNodes.add(tmp);
					}
				}
				else
				{
					BTreeIndex tmp = (BTreeIndex)node;
					if (i == 0)
					{
						if (tmp.mMap.get(1, new ArrayMapEntry()).getKey().compareTo(high) <= 0)
						{
							mIndexNodes.add(tmp);
						}
					}
					else
					{
						System.out.println(tmp.mMap.getLast().getKey()+" <= "+high);
						if (tmp.mMap.get(1, new ArrayMapEntry()).getKey().compareTo(high) <= 0 && tmp.mMap.getLast().getKey().compareTo(high) <= 0)
						{
							mIndexNodes.add(tmp);
						}
					}
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
