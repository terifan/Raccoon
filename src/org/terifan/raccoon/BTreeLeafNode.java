package org.terifan.raccoon;

import static org.terifan.raccoon.RaccoonCollection.TYPE_TREENODE;
import static org.terifan.raccoon.BTree.BLOCKPOINTER_PLACEHOLDER;
import org.terifan.raccoon.ArrayMap.PutResult;
import static org.terifan.raccoon.RaccoonCollection.TYPE_EXTERNAL;
import org.terifan.raccoon.RuntimeDiagnostics.Operation;
import org.terifan.raccoon.util.Result;
import org.terifan.raccoon.blockdevice.BlockType;


public class BTreeLeafNode extends BTreeNode
{
	protected ArrayMap mMap;


	BTreeLeafNode(BTree aTree, ArrayMap aMap)
	{
		super(aTree, 0);

		mMap = aMap;
	}


	@Override
	boolean get(ArrayMapKey aKey, ArrayMapEntry aEntry)
	{
		return mMap.get(aEntry);
	}


	@Override
	PutResult put(ArrayMapKey aKey, ArrayMapEntry aEntry, Result<ArrayMapEntry> aResult)
	{
		mModified = true;
		return mMap.insert(aEntry, aResult);
	}


	@Override
	RemoveResult remove(ArrayMapKey aKey, Result<ArrayMapEntry> aOldEntry)
	{
		if (mMap.remove(aKey, aOldEntry))
		{
			mModified = true;
			return RemoveResult.REMOVED;
		}

		return RemoveResult.NO_MATCH;
	}


	@Override
	void visit(BTreeVisitor aVisitor, ArrayMapKey aLowestKey, ArrayMapKey aHighestKey)
	{
		if (aVisitor.beforeAnyNode(this))
		{
			if (aVisitor.beforeLeafNode(this))
			{
				mHighlight = BTree.RECORD_USE;

				aVisitor.leaf(this);
			}
		}
	}


	@Override
	SplitResult split()
	{
		mTree.freeBlock(mBlockPointer);

		ArrayMap[] maps = mMap.split(mTree.getLeafSize());

		BTreeLeafNode left = new BTreeLeafNode(mTree, maps[0]);
		BTreeLeafNode rigt = new BTreeLeafNode(mTree, maps[1]);
		left.mModified = true;
		rigt.mModified = true;

		return new SplitResult(left, rigt, left.mMap.getFirst().getKey(), rigt.mMap.getFirst().getKey());
	}


	BTreeInteriorNode upgrade()
	{
		mTree.freeBlock(mBlockPointer);

		ArrayMap[] maps = mMap.split(mTree.getLeafSize());

		BTreeLeafNode left = new BTreeLeafNode(mTree, maps[0]);
		BTreeLeafNode rigt = new BTreeLeafNode(mTree, maps[1]);
		left.mModified = true;
		rigt.mModified = true;

		ArrayMapKey keyLeft = ArrayMapKey.EMPTY;
		ArrayMapKey keyRigt = rigt.mMap.getKey(0);

		BTreeInteriorNode newInterior = new BTreeInteriorNode(mTree, 1, new ArrayMap(mTree.getNodeSize()));
		newInterior.mModified = true;
		newInterior.mChildNodes.putEntry(new ArrayMapEntry(keyLeft, BLOCKPOINTER_PLACEHOLDER, TYPE_TREENODE));
		newInterior.mChildNodes.putEntry(new ArrayMapEntry(keyRigt, BLOCKPOINTER_PLACEHOLDER, TYPE_TREENODE));
		newInterior.mChildNodes.put(keyLeft, left);
		newInterior.mChildNodes.put(keyRigt, rigt);

		return newInterior;
	}


	@Override
	boolean commit()
	{
		if (mModified)
		{
			RuntimeDiagnostics.collectStatistics(Operation.FREE_LEAF, mBlockPointer);
			RuntimeDiagnostics.collectStatistics(Operation.WRITE_LEAF, 1);

			mTree.freeBlock(mBlockPointer);

			mBlockPointer = mTree.writeBlock(mMap.array(), 0, BlockType.BTREE_LEAF);
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
		return String.format("BTreeLeaf{mMap=" + mMap + '}');
	}


	@Override
	protected String integrityCheck()
	{
		return mMap.integrityCheck();
	}


	@Override
	protected int size()
	{
		return mMap.size();
	}


	@Override
	protected void scan(ScanResult aScanResult)
	{
		int fillRatio = mMap.getUsedSpace() * 100 / mTree.getLeafSize();

		aScanResult.log.append("{" + (mBlockPointer == null ? "" : mBlockPointer.getBlockIndex0()) + ":" + fillRatio + "%" + "}");
		aScanResult.log.append("[");

		boolean first = true;

		for (ArrayMapEntry entry : mMap)
		{
			if (!first)
			{
				aScanResult.log.append(",");
			}
			first = false;
			if (entry.getType() == TYPE_EXTERNAL)
			{
				aScanResult.log.append("'" + stringifyKey(entry.getKey()) + "'");
			}
			else
			{
				aScanResult.log.append("'" + stringifyKey(entry.getKey()) + "'");
			}
		}

		aScanResult.log.append("]");

		if (mHighlight)
		{
			aScanResult.log.append("#a00#a00#fff");
		}
		else if (fillRatio > 100)
		{
			aScanResult.log.append(mModified ? "#a00#a00#fff" : "#666#666#fff");
		}
		else
		{
			aScanResult.log.append(mModified ? "#f00#f00#fff" : "#888#fff#000");
		}
	}
}
