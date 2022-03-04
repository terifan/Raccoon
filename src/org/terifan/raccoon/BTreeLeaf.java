package org.terifan.raccoon;

import static org.terifan.raccoon.BTreeTableImplementation.POINTER_PLACEHOLDER;
import static org.terifan.raccoon.BTreeTableImplementation.mIndexSize;
import static org.terifan.raccoon.BTreeTableImplementation.mLeafSize;
import org.terifan.raccoon.util.Result;


public class BTreeLeaf extends BTreeNode
{
	BTreeLeaf()
	{
	}


	@Override
	boolean put(BTreeIndex aParent, ArrayMapEntry aEntry, MarshalledKey aKey, Result<ArrayMapEntry> aResult)
	{
		return !mMap.insert(aEntry, aResult);
	}


	@Override
	BTreeNode[] split()
	{
		ArrayMap[] maps = mMap.split();

		BTreeLeaf a = new BTreeLeaf();
		BTreeLeaf b = new BTreeLeaf();
		a.mMap = maps[0];
		b.mMap = maps[1];

		return new BTreeNode[]
		{
			a, b
		};
	}


	BTreeIndex upgrade()
	{
		ArrayMap[] maps = mMap.split();

		BTreeLeaf a = new BTreeLeaf();
		BTreeLeaf b = new BTreeLeaf();
		a.mMap = maps[0];
		b.mMap = maps[1];

		ArrayMapEntry midKeyBytes = new ArrayMapEntry();
		b.mMap.get(0, midKeyBytes);

		MarshalledKey keyB = new MarshalledKey(new byte[0]);
		MarshalledKey keyA = new MarshalledKey(midKeyBytes.getKey());

		BTreeIndex newIndex = new BTreeIndex();
		newIndex.mMap = new ArrayMap(mIndexSize);
		newIndex.mMap.put(new ArrayMapEntry(keyA.key, POINTER_PLACEHOLDER, (byte)0), null);
		newIndex.mMap.put(new ArrayMapEntry(keyB.key, POINTER_PLACEHOLDER, (byte)0), null);
		newIndex.mChildren.put(keyA, a);
		newIndex.mChildren.put(keyB, b);

		return newIndex;
	}
}
