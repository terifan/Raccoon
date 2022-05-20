package org.terifan.raccoon;

import java.util.Random;
import static org.terifan.raccoon.BTreeTableImplementation.POINTER_PLACEHOLDER;
import static org.terifan.raccoon.BTreeTableImplementation.mIndexSize;
import org.terifan.raccoon.util.Result;


public class BTreeLeaf extends BTreeNode
{
	BTreeLeaf(BTreeTableImplementation aImplementation)
	{
		super(aImplementation);
	}


	@Override
	boolean get(MarshalledKey aKey, ArrayMapEntry aEntry)
	{
		return mMap.get(aEntry);
	}


	@Override
	boolean put(BTreeIndex aParent, MarshalledKey aKey, ArrayMapEntry aEntry, Result<ArrayMapEntry> aResult)
	{
		mModified =  true;

		boolean b = !mMap.insert(aEntry, aResult);

		if (new Random().nextBoolean())
			commit();

		return b;
	}


	@Override
	BTreeNode[] split()
	{
		ArrayMap[] maps = mMap.split(BTreeTableImplementation.mLeafSize);

		BTreeLeaf a = new BTreeLeaf(mImplementation);
		BTreeLeaf b = new BTreeLeaf(mImplementation);
		a.mMap = maps[0];
		b.mMap = maps[1];
		a.mModified = true;
		b.mModified = true;

		return new BTreeNode[]
		{
			a, b
		};
	}


	BTreeIndex upgrade()
	{
		ArrayMap[] maps = mMap.split(BTreeTableImplementation.mLeafSize);

		BTreeLeaf a = new BTreeLeaf(mImplementation);
		BTreeLeaf b = new BTreeLeaf(mImplementation);
		a.mMap = maps[0];
		b.mMap = maps[1];
		a.mModified = true;
		b.mModified = true;

		ArrayMapEntry ak = new ArrayMapEntry();
		a.mMap.get(0, ak);

		ArrayMapEntry bk = new ArrayMapEntry();
		b.mMap.get(0, bk);

		MarshalledKey keyA = MarshalledKey.unmarshall(new byte[0]);
		MarshalledKey keyB = MarshalledKey.unmarshall(bk.getKey());

		BTreeIndex newIndex = new BTreeIndex(mImplementation);
		newIndex.mMap = new ArrayMap(mIndexSize);
		newIndex.mMap.put(new ArrayMapEntry(keyA.marshall(), POINTER_PLACEHOLDER, (byte)0x77), null);
		newIndex.mMap.put(new ArrayMapEntry(keyB.marshall(), POINTER_PLACEHOLDER, (byte)0x66), null);
		newIndex.mChildren.put(keyA, a);
		newIndex.mChildren.put(keyB, b);
		newIndex.mModified = true;

		return newIndex;
	}


	@Override
	boolean commit()
	{
		if (mModified)
		{
			mImplementation.freeBlock(mBlockPointer);

			mBlockPointer = mImplementation.writeBlock(mMap.array(), BlockType.LEAF);
		}

		return mModified;
	}


	@Override
	public String toString()
	{
		return "BTreeLeaf{" + mMap + '}';
	}
}
