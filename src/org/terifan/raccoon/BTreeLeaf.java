package org.terifan.raccoon;

import org.terifan.raccoon.ArrayMap.InsertResult;
import static org.terifan.raccoon.BTreeTableImplementation.POINTER_PLACEHOLDER;
import org.terifan.raccoon.util.Result;
import static org.terifan.raccoon.BTreeTableImplementation.INDEX_SIZE;


public class BTreeLeaf extends BTreeNode
{
	BTreeLeaf(BTreeTableImplementation aImplementation, BTreeIndex aParent)
	{
		super(aImplementation, aParent, 0);
	}


	@Override
	boolean get(MarshalledKey aKey, ArrayMapEntry aEntry)
	{
		return mMap.get(aEntry);
	}


	@Override
	InsertResult put(MarshalledKey aKey, ArrayMapEntry aEntry, Result<ArrayMapEntry> aResult)
	{
		mModified = true;

		return mMap.insert(aEntry, aResult);
	}


	@Override
	RemoveResult remove(MarshalledKey aKey, Result<ArrayMapEntry> aOldEntry)
	{
		boolean removed = mMap.remove(aKey.array(), aOldEntry);

		if (removed)
		{
			mModified = true;
		}

		return removed ? RemoveResult.OK : RemoveResult.NONE;
	}


	@Override
	SplitResult split()
	{
		mImplementation.freeBlock(mBlockPointer);

		ArrayMap[] maps = mMap.split(BTreeTableImplementation.LEAF_SIZE);

		BTreeLeaf a = new BTreeLeaf(mImplementation, mParent);
		BTreeLeaf b = new BTreeLeaf(mImplementation, mParent);
		a.mMap = maps[0];
		b.mMap = maps[1];
		a.mModified = true;
		b.mModified = true;

		return new SplitResult(a, b, new MarshalledKey(a.mMap.getFirst().getKey()), new MarshalledKey(b.mMap.getFirst().getKey()));
	}


	BTreeIndex upgrade()
	{
		mImplementation.freeBlock(mBlockPointer);

		ArrayMap[] maps = mMap.split(BTreeTableImplementation.LEAF_SIZE);

		BTreeLeaf a = new BTreeLeaf(mImplementation, mParent);
		BTreeLeaf b = new BTreeLeaf(mImplementation, mParent);
		a.mMap = maps[0];
		b.mMap = maps[1];
		a.mModified = true;
		b.mModified = true;

		byte[] key = b.mMap.getKey(0);

		MarshalledKey keyA = new MarshalledKey(new byte[0]);
		MarshalledKey keyB = new MarshalledKey(key);

		BTreeIndex newIndex = new BTreeIndex(mImplementation, mParent, 1);
		newIndex.mMap = new ArrayMap(INDEX_SIZE);
		newIndex.mMap.put(new ArrayMapEntry(keyA.array(), POINTER_PLACEHOLDER, (byte)0x77), null);
		newIndex.mMap.put(new ArrayMapEntry(keyB.array(), POINTER_PLACEHOLDER, (byte)0x66), null);
		newIndex.mBuffer.put(keyA, a);
		newIndex.mBuffer.put(keyB, b);
		newIndex.mModified = true;

		return newIndex;
	}


	@Override
	boolean commit()
	{
		if (mModified)
		{
			mImplementation.freeBlock(mBlockPointer);

			mBlockPointer = mImplementation.writeBlock(mMap.array(), 0, BlockType.LEAF);
		}

		return mModified;
	}


	@Override
	public String toString()
	{
		return "BTreeLeaf{mMap=" + mMap + '}';
	}
}
