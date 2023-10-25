package org.terifan.raccoon;

import org.terifan.raccoon.blockdevice.BlockType;
import static org.terifan.raccoon.RaccoonCollection.TYPE_TREENODE;
import static org.terifan.raccoon.BTree.BLOCKPOINTER_PLACEHOLDER;
import org.terifan.raccoon.ArrayMap.PutResult;
import static org.terifan.raccoon.BTree.INT_BLOCK_SIZE;
import static org.terifan.raccoon.BTree.LEAF_BLOCK_SIZE;
import org.terifan.raccoon.RuntimeDiagnostics.Operation;
import org.terifan.raccoon.blockdevice.util.Console;
import org.terifan.raccoon.util.Result;


public class BTreeLeafNode extends BTreeNode
{
	BTreeLeafNode()
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
	void visit(BTree aImplementation, BTreeVisitor aVisitor, ArrayMapKey aLowestKey, ArrayMapKey aHighestKey)
	{
		if (aVisitor.beforeAnyNode(aImplementation, this))
		{
			if (aVisitor.beforeLeafNode(aImplementation, this))
			{
				mHighlight = BTree.RECORD_USE;

				aVisitor.leaf(aImplementation, this);
			}
		}
	}


	@Override
	SplitResult split(BTree aImplementation)
	{
		aImplementation.freeBlock(mBlockPointer);

		ArrayMap[] maps = mMap.split(aImplementation.getConfiguration().getInt(LEAF_BLOCK_SIZE));

		BTreeLeafNode left = new BTreeLeafNode();
		BTreeLeafNode rigt = new BTreeLeafNode();
		left.mMap = maps[0];
		rigt.mMap = maps[1];
		left.mModified = true;
		rigt.mModified = true;

		return new SplitResult(left, rigt, left.mMap.getFirst().getKey(), rigt.mMap.getFirst().getKey());
	}


	BTreeInteriorNode upgrade(BTree aImplementation)
	{
		aImplementation.freeBlock(mBlockPointer);

		ArrayMap[] maps = mMap.split(aImplementation.getConfiguration().getInt(LEAF_BLOCK_SIZE));

		BTreeLeafNode left = new BTreeLeafNode();
		BTreeLeafNode rigt = new BTreeLeafNode();
		left.mMap = maps[0];
		rigt.mMap = maps[1];
		left.mModified = true;
		rigt.mModified = true;

		ArrayMapKey keyLeft = ArrayMapKey.EMPTY;
		ArrayMapKey keyRigt = rigt.mMap.getKey(0);

		BTreeInteriorNode newInterior = new BTreeInteriorNode(1);
		newInterior.mModified = true;
		newInterior.mMap = new ArrayMap(aImplementation.getConfiguration().getInt(INT_BLOCK_SIZE));
		newInterior.mMap.put(new ArrayMapEntry(keyLeft, BLOCKPOINTER_PLACEHOLDER, TYPE_TREENODE), null);
		newInterior.mMap.put(new ArrayMapEntry(keyRigt, BLOCKPOINTER_PLACEHOLDER, TYPE_TREENODE), null);
		newInterior.mChildNodes.put(keyLeft, left);
		newInterior.mChildNodes.put(keyRigt, rigt);

		return newInterior;
	}


	@Override
	boolean commit(BTree aImplementation)
	{
		if (mModified)
		{
			RuntimeDiagnostics.collectStatistics(Operation.FREE_LEAF, mBlockPointer);
			RuntimeDiagnostics.collectStatistics(Operation.WRITE_LEAF, 1);

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
