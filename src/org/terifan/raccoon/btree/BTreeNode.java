package org.terifan.raccoon.btree;

import org.terifan.raccoon.blockdevice.BlockPointer;


public abstract class BTreeNode
{
	BTree mTree;
	BTreeInteriorNode mParent;
	BlockPointer mBlockPointer;
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


	protected abstract String integrityCheck();


	protected abstract int size();
}
