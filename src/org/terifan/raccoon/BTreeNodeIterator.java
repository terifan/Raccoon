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
					ArrayMapKey low = new ArrayMapKey("33");
					ArrayMapKey high = new ArrayMapKey("68");

		while (mLeafNodes.isEmpty() && !mIndexNodes.isEmpty())
		{
			BTreeIndex indexNode = mIndexNodes.remove(0);
			for (int i = 0, sz = indexNode.size(); i < sz; i++)
			{
				BTreeNode node = indexNode.getNode(mImplementation, i);
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
					// * 10 20 30 40 50
					// 5 - 25

					BTreeIndex tmp = (BTreeIndex)node;
					System.out.println(tmp.mMap);
					System.out.println(tmp.mMap.get(1, new ArrayMapEntry()).getKey());
					System.out.println(low);
//					if (tmp.mMap.get(1, new ArrayMapEntry()).getKey().compareTo(low) >= 0 && tmp.mMap.getFirst().getKey().compareTo(high) <= 0)
					if (tmp.mMap.getLast().getKey().compareTo(high) >= 0 && tmp.mMap.getFirst().getKey().compareTo(high) <= 0)
					{
					System.out.println("------->"+tmp);
						mIndexNodes.add(tmp);
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
