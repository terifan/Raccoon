package org.terifan.raccoon.btree;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import org.terifan.raccoon.storage.BlockPointer;
import org.terifan.raccoon.util.ByteArrayBuffer;


public class BTreeNodeIterator implements Iterator<BTreeLeaf>
{
	private ArrayList<Integer> mPosition;
	private HashMap<Integer, BTreeIndex> mIndex;
	private BTreeLeaf mNext;


	BTreeNodeIterator(BTreeNode aRoot)
	{
		if (aRoot instanceof BTreeLeaf)
		{
			mNext = (BTreeLeaf)aRoot;
		}
		else
		{
			mIndex = new HashMap<>();
			mIndex.put(0, (BTreeIndex)aRoot);
			mPosition = new ArrayList<>();
			mPosition.add(0);
		}
	}

	//         /--- a
	//   /----o---- b
	//   |     \--- c
	//   |     /--- d
	// --o----o---- e
	//   |     \--- f
	//   |     /--- g
	//   \----o---- h
	//         \--- i

	@Override
	public boolean hasNext()
	{
		if (mNext != null)
		{
			return true;
		}

//		ArrayMapEntry entry = new ArrayMapEntry();
//
//		ArrayMap map = mIndex.get(mIndex.size() - 1).mMap;
//		int pos = mPosition.get(mPosition.size() - 1);
//
//		map.get(pos, entry);

		return false;
	}


	@Override
	public BTreeLeaf next()
	{
		BTreeLeaf tmp = mNext;
		mNext = null;
		return tmp;
	}
}
