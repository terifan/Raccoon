package org.terifan.raccoon.btree;

import java.util.function.Consumer;
import org.terifan.raccoon.RuntimeDiagnostics;
import org.terifan.raccoon.RuntimeDiagnostics.Operation;
import org.terifan.raccoon.blockdevice.BlockType;


public class BTreeLeafNode extends BTreeNode
{
	ArrayMap mMap;


	BTreeLeafNode(BTree aTree, BTreeInteriorNode aParent, ArrayMap aMap)
	{
		super(aTree, aParent, 0);

		mMap = aMap;
	}


	@Override
	OpResult get(ArrayMapKey aKey)
	{
		return mMap.get(aKey);
	}


	@Override
	OpResult put(ArrayMapKey aKey, ArrayMapEntry aEntry)
	{
//		mModified = true;
		OpResult result = mMap.insert(aEntry);

		if (mMap.getCapacity() > mTree.getConfiguration().getLeafSize())
		{
			mTree.schedule(this);
		}

		return result;
	}


	@Override
	OpResult remove(ArrayMapKey aKey)
	{
		OpResult result = mMap.remove(aKey);

//		if (mMap.getCapacity() < mTree.getConfiguration().getLeafSize() / 2)
//		{
//			mTree.schedule(this);
//		}

//		mModified |= result.state == OpState.DELETE;

		return result;
	}


	@Override
	void visit(BTreeVisitor aVisitor, ArrayMapKey aLowestKey, ArrayMapKey aHighestKey)
	{
		if (aVisitor.beforeAnyNode(this))
		{
			if (aVisitor.beforeLeafNode(this))
			{
//				mHighlight = BTree.RECORD_USE;

				aVisitor.leaf(this);
			}
		}
	}


	@Override
	void commit()
	{
//		if (mModified)
//		{
			RuntimeDiagnostics.collectStatistics(Operation.FREE_LEAF, mBlockPointer);
			RuntimeDiagnostics.collectStatistics(Operation.WRITE_LEAF, 1);

			mTree.freeBlock(mBlockPointer);

			mBlockPointer = mTree.writeBlock(mMap.array(), 0, BlockType.BTREE_LEAF);
//		}

//		return mModified;
	}


	@Override
	protected void postCommit()
	{
//		mModified = false;
	}


	@Override
	public String toString()
	{
		return String.format("BTreeLeaf{"+UNIQUE+", mMap=" + mMap + '}');
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


	public void forEachEntry(Consumer<? super ArrayMapEntry> aConsumer)
	{
		mMap.forEach(aConsumer);
	}
}
