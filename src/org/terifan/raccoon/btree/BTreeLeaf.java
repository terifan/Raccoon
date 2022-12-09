package org.terifan.raccoon.btree;

import org.terifan.raccoon.BlockType;
import org.terifan.raccoon.TransactionGroup;
import org.terifan.raccoon.btree.ArrayMap.PutResult;
import org.terifan.raccoon.util.Result;
import static org.terifan.raccoon.btree.BTree.INDEX_SIZE;
import static org.terifan.raccoon.btree.BTree.BLOCKPOINTER_PLACEHOLDER;
import org.terifan.raccoon.util.Console;
import org.terifan.raccoon.util.Log;


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

		ArrayMap[] maps = mMap.split(BTree.LEAF_SIZE);

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

		ArrayMap[] maps = mMap.split(BTree.LEAF_SIZE);

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
		newIndex.mMap = new ArrayMap(INDEX_SIZE);
		newIndex.mMap.put(new ArrayMapEntry(keyA.array(), BLOCKPOINTER_PLACEHOLDER), null);
		newIndex.mMap.put(new ArrayMapEntry(keyB.array(), BLOCKPOINTER_PLACEHOLDER), null);
		newIndex.mChildNodes.put(keyA, a);
		newIndex.mChildNodes.put(keyB, b);

		return newIndex;
	}


	@Override
	boolean commit(BTree aImplementation, TransactionGroup aTransactionGroup)
	{
		if (mModified)
		{
			aImplementation.freeBlock(mBlockPointer);

			mBlockPointer = aImplementation.writeBlock(aTransactionGroup, mMap.array(), 0, BlockType.LEAF);

			aImplementation.hasCommitted(this);
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
