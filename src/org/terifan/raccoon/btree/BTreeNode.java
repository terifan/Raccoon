package org.terifan.raccoon.btree;

import org.terifan.raccoon.blockdevice.BlockPointer;


public abstract class BTreeNode
{
	BTree mTree;
	BTreeInteriorNode mParent;
	BlockPointer mBlockPointer;
	ArrayMap mMap;
	boolean mModified;
	int mLevel;


	protected BTreeNode(BTree aTree, BTreeInteriorNode aParent, int aLevel)
	{
		mTree = aTree;
		mParent = aParent;
		mLevel = aLevel;
	}


	abstract void get(ArrayMapEntry aEntry);


	abstract void put(ArrayMapEntry aEntry);


	abstract void remove(ArrayMapEntry aEntry);


	abstract void visit(BTreeVisitor aVisitor, ArrayMapEntry aLowestKey, ArrayMapEntry aHighestKey);


	abstract boolean persist();


	public int size()
	{
		return mMap.size();
	}


	@Override
	public String toString()
	{
		return String.format("%s{mLevel=%d, fill=%5.1f, keys=%s}", getClass().getSimpleName(), mLevel, mMap.getUsedSpace() * 100.0 / mMap.getCapacity(), mMap.keys(k -> "\"" + k.toString() + "\""));
	}


	protected String integrityCheck()
	{
		return mMap.integrityCheck();
	}
}
