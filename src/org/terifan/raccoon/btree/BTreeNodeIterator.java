package org.terifan.raccoon.btree;

import java.util.ArrayList;
import java.util.Iterator;
import static org.terifan.raccoon.RaccoonCollection.TYPE_TREENODE;
import org.terifan.raccoon.storage.BlockPointer;


public class BTreeNodeIterator implements Iterator<BTreeLeaf>
{
	private BTree mTree;
	private ArrayList<BTreeNode> mNodes;
	private ArrayList<Iterator<ArrayMapEntry>> mIterators;
	private BTreeLeaf mPending;


	BTreeNodeIterator(BTree aTree, BTreeNode aRoot)
	{
		mTree = aTree;

		mNodes = new ArrayList<>();
		mNodes.add(aRoot);

		mIterators = new ArrayList<>();
		mIterators.add(aRoot.mMap.iterator());
	}


//      /-- a
//   /--o-- b
//   |  \-- c
//   |  /-- d
// --o--o-- e
//   |  \-- f
//   |  /-- g
//   \--o-- h
//      \-- i
	@Override
	public boolean hasNext()
	{
		if (mPending != null)
		{
			return true;
		}

		while (!mIterators.isEmpty())
		{
			Iterator<ArrayMapEntry> it = mIterators.get(mIterators.size() - 1);

			if (!it.hasNext())
			{
				mIterators.remove(mIterators.size() - 1);
				mNodes.remove(mNodes.size() - 1);
				continue;
			}

			ArrayMapEntry entry = it.next();
			BTreeNode node = mNodes.get(mNodes.size() - 1);

			if (node instanceof BTreeIndex)
			{
				node = ((BTreeIndex)node).getNode(mTree, entry);
				mIterators.add(node.mMap.iterator());
				mNodes.add(node);
			}
			else
			{
				mPending = (BTreeLeaf)node;
				return true;
			}
		}

		return false;
	}


	@Override
	public BTreeLeaf next()
	{
		BTreeLeaf tmp = mPending;
		mPending = null;
		return tmp;
	}
}
