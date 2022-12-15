package org.terifan.raccoon;

import org.terifan.raccoon.BlockType;
import static org.terifan.raccoon.RaccoonCollection.TYPE_TREENODE;
import org.terifan.raccoon.ArrayMap.PutResult;
import org.terifan.raccoon.util.Result;
import static org.terifan.raccoon.BTree.BLOCKPOINTER_PLACEHOLDER;
import org.terifan.raccoon.util.Console;


public class BTreeLeaf extends BTreeNode
{
	BTreeLeaf()
	{
		super(0);
	}


	@Override
	boolean get(BTree aImplementation, MarshalledKey aKey, ArrayMapEntry aEntry)
	{
		return mMap.get(aEntry);
	}


	@Override
	PutResult put(BTree aImplementation, MarshalledKey aKey, ArrayMapEntry aEntry, Result<ArrayMapEntry> aResult)
	{
		mModified = true;
		return mMap.insert(aEntry, aResult);
	}


	@Override
	RemoveResult remove(BTree aImplementation, MarshalledKey aKey, Result<ArrayMapEntry> aOldEntry)
	{
		boolean removed = mMap.remove(aKey.array(), aOldEntry);

		if (removed)
		{
			mModified = true;
		}

		return removed ? RemoveResult.REMOVED : RemoveResult.NO_MATCH;
	}


	@Override
	SplitResult split(BTree aImplementation)
	{
		aImplementation.freeBlock(mBlockPointer);

		ArrayMap[] maps = mMap.split(aImplementation.getConfiguration().getInt("leafSize"));

		BTreeLeaf a = new BTreeLeaf();
		BTreeLeaf b = new BTreeLeaf();
		a.mMap = maps[0];
		b.mMap = maps[1];
		a.mModified = true;
		b.mModified = true;

		return new SplitResult(a, b, new MarshalledKey(a.mMap.getFirst().getKey()), new MarshalledKey(b.mMap.getFirst().getKey()));
	}


	BTreeIndex upgrade(BTree aImplementation)
	{
		aImplementation.freeBlock(mBlockPointer);

		ArrayMap[] maps = mMap.split(aImplementation.getConfiguration().getInt("leafSize"));

		BTreeLeaf a = new BTreeLeaf();
		BTreeLeaf b = new BTreeLeaf();
		a.mMap = maps[0];
		b.mMap = maps[1];
		a.mModified = true;
		b.mModified = true;

		MarshalledKey keyA = new MarshalledKey(new byte[0]);
		MarshalledKey keyB = new MarshalledKey(b.mMap.getKey(0));

		BTreeIndex newIndex = new BTreeIndex(1);
		newIndex.mModified = true;
		newIndex.mMap = new ArrayMap(aImplementation.getConfiguration().getInt("indexSize"));
		newIndex.mMap.put(new ArrayMapEntry(keyA.array(), BLOCKPOINTER_PLACEHOLDER, TYPE_TREENODE), null);
		newIndex.mMap.put(new ArrayMapEntry(keyB.array(), BLOCKPOINTER_PLACEHOLDER, TYPE_TREENODE), null);
		newIndex.mChildNodes.put(keyA, a);
		newIndex.mChildNodes.put(keyB, b);

		return newIndex;
	}


	@Override
	boolean commit(BTree aImplementation)
	{
		if (mModified)
		{
			aImplementation.freeBlock(mBlockPointer);

			mBlockPointer = aImplementation.writeBlock(mMap.array(), 0, BlockType.TREE_LEAF);
		}

		return mModified;
	}


	@Override
	protected void postCommit()
	{
		mModified = false;
	}


	@Override
	public String toString()
	{
		return Console.format("BTreeLeaf{mMap=" + mMap + '}');
	}
}
