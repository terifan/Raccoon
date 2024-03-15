package org.terifan.raccoon.btree;

import org.terifan.raccoon.blockdevice.BlockPointer;


public abstract class BTreeNode
{
	protected BTree mTree;
	protected BTreeInteriorNode mParent;
	protected BlockPointer mBlockPointer;
	protected int mLevel;


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


	abstract void commit();


	protected abstract void postCommit();


	protected abstract String integrityCheck();


	protected abstract int size();


//	@Override
//	public int hashCode()
//	{
//		return mBlockPointer.hashCode();
//	}
//
//
//	@Override
//	public boolean equals(Object aObj)
//	{
//		if (aObj instanceof BlockPointer v)
//		{
//			return mBlockPointer.equals(v);
//		}
//		return false;
//	}


	enum VisitorState
	{
		CONTINUE,
		ABORT,
		SKIP
	}
}
