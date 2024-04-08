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
	void get(ArrayMapEntry aEntry)
	{
		mMap.get(aEntry);
	}


	@Override
	void put(ArrayMapEntry aEntry)
	{
		mMap.insert(aEntry);
		mModified = true;

		if (mMap.getCapacity() > mTree.getConfiguration().getLeafSize())
		{
			mTree.schedule(this);
		}
	}


	@Override
	void remove(ArrayMapEntry aEntry)
	{
		mMap.remove(aEntry);
		mModified = true;

		if (mMap.getCapacity() < mTree.getConfiguration().getLeafSize() / 4)
		{
			mTree.schedule(this);
		}
	}


	@Override
	void visit(BTreeVisitor aVisitor, ArrayMapEntry aLowestKey, ArrayMapEntry aHighestKey)
	{
		if (aVisitor.beforeAnyNode(this))
		{
			if (aVisitor.beforeLeafNode(this))
			{
				aVisitor.leaf(this);
			}
		}
	}


	@Override
	boolean persist()
	{
		if (!mModified)
		{
			assert mBlockPointer != null;
			return false;
		}

		RuntimeDiagnostics.collectStatistics(Operation.FREE_LEAF, mBlockPointer);
		RuntimeDiagnostics.collectStatistics(Operation.WRITE_LEAF, 1);

		mTree.freeBlock(mBlockPointer);
		mBlockPointer = mTree.writeBlock(mMap.array(), 0, BlockType.BTREE_LEAF);
		mModified = false;

		return true;
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


	public void forEachEntry(Consumer<? super ArrayMapEntry> aConsumer)
	{
		mMap.forEach(aConsumer);
	}
}
