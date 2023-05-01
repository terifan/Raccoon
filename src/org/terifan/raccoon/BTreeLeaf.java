package org.terifan.raccoon;

import static org.terifan.raccoon.RaccoonCollection.TYPE_TREENODE;
import static org.terifan.raccoon.BTree.BLOCKPOINTER_PLACEHOLDER;
import org.terifan.raccoon.ArrayMap.PutResult;
import org.terifan.raccoon.RuntimeDiagnostics.Operation;
import org.terifan.raccoon.blockdevice.util.Console;
import org.terifan.raccoon.util.Result;


public class BTreeLeaf extends BTreeNode
{
	BTreeLeaf()
	{
		super(0);
	}


	@Override
	boolean get(BTree aImplementation, ArrayMapKey aKey, ArrayMapEntry aEntry)
	{
		return mMap.get(aEntry);
	}


	@Override
	PutResult put(BTree aImplementation, ArrayMapKey aKey, ArrayMapEntry aEntry, Result<ArrayMapEntry> aResult)
	{
		mModified = true;
		return mMap.insert(aEntry, aResult);
	}


	@Override
	RemoveResult remove(BTree aImplementation, ArrayMapKey aKey, Result<ArrayMapEntry> aOldEntry)
	{
		boolean removed = mMap.remove(aKey, aOldEntry);

		if (removed)
		{
			mModified = true;
		}

		return removed ? RemoveResult.REMOVED : RemoveResult.NO_MATCH;
	}


	@Override
	void visit(BTree aImplementation, BTreeVisitor aVisitor)
	{
		aVisitor.anyNode(aImplementation, this);
		aVisitor.leaf(aImplementation, this);
	}


	@Override
	SplitResult split(BTree aImplementation)
	{
		aImplementation.freeBlock(mBlockPointer);

		ArrayMap[] maps = mMap.split(aImplementation.getConfiguration().getInt("leafBlockSize"));

		BTreeLeaf a = new BTreeLeaf();
		BTreeLeaf b = new BTreeLeaf();
		a.mMap = maps[0];
		b.mMap = maps[1];
		a.mModified = true;
		b.mModified = true;

		return new SplitResult(a, b, a.mMap.getFirst().getKey(), b.mMap.getFirst().getKey());
	}


	BTreeIndex upgrade(BTree aImplementation)
	{
		aImplementation.freeBlock(mBlockPointer);

		ArrayMap[] maps = mMap.split(aImplementation.getConfiguration().getInt("leafBlockSize"));

		BTreeLeaf a = new BTreeLeaf();
		BTreeLeaf b = new BTreeLeaf();
		a.mMap = maps[0];
		b.mMap = maps[1];
		a.mModified = true;
		b.mModified = true;

		ArrayMapKey keyA = ArrayMapKey.EMPTY;
		ArrayMapKey keyB = b.mMap.getKey(0);

		BTreeIndex newIndex = new BTreeIndex(1);
		newIndex.mModified = true;
		newIndex.mMap = new ArrayMap(aImplementation.getConfiguration().getInt("intBlockSize"));
		newIndex.mMap.put(new ArrayMapEntry(keyA, BLOCKPOINTER_PLACEHOLDER, TYPE_TREENODE), null);
		newIndex.mMap.put(new ArrayMapEntry(keyB, BLOCKPOINTER_PLACEHOLDER, TYPE_TREENODE), null);
		newIndex.mChildNodes.put(keyA, a);
		newIndex.mChildNodes.put(keyB, b);

		return newIndex;
	}


	@Override
	boolean commit(BTree aImplementation)
	{
		if (mModified)
		{
			assert RuntimeDiagnostics.collectStatistics(Operation.FREE_LEAF, mBlockPointer);
			assert RuntimeDiagnostics.collectStatistics(Operation.WRITE_LEAF, 1);

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
