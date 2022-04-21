package org.terifan.raccoon;

import static org.terifan.raccoon.BTreeTableImplementation.POINTER_PLACEHOLDER;
import static org.terifan.raccoon.BTreeTableImplementation.mIndexSize;
import org.terifan.raccoon.util.Result;


public class BTreeLeaf extends BTreeNode
{
	BTreeLeaf()
	{
	}


	@Override
	boolean put(BTreeIndex aParent, ArrayMapEntry aEntry, Result<ArrayMapEntry> aResult)
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

		ArrayMapEntry ak = new ArrayMapEntry();
		a.mMap.get(0, ak);

		ArrayMapEntry bk = new ArrayMapEntry();
		b.mMap.get(0, bk);

//		ArrayMapEntry midKeyBytes = new ArrayMapEntry();
//		b.mMap.get(0, midKeyBytes);
//
//		MarshalledKey keyA = new MarshalledKey(true);
//		MarshalledKey keyB = MarshalledKey.unmarshall(midKeyBytes.getKey());

		MarshalledKey keyA = MarshalledKey.unmarshall(ak.getKey());
		MarshalledKey keyB = MarshalledKey.unmarshall(bk.getKey());

		BTreeIndex newIndex = new BTreeIndex();
		newIndex.mMap = new ArrayMap(mIndexSize);
		newIndex.mMap.put(new ArrayMapEntry(keyA.marshall(), POINTER_PLACEHOLDER, (byte)0x77), null);
		newIndex.mMap.put(new ArrayMapEntry(keyB.marshall(), POINTER_PLACEHOLDER, (byte)0x66), null);
		newIndex.mChildren.put(keyA, a);
		newIndex.mChildren.put(keyB, b);

		return newIndex;
	}
}
