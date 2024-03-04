package org.terifan.raccoon.btree;

import java.util.Iterator;
import org.terifan.raccoon.document.Document;


public class BTreeIterator implements Iterator<Document>
{
	private BTree mTree;
	private boolean mClosed;
	private Document mNext;
	private ArrayMapKey mLastKey;
	private BTreeLeafNode mLeafNode;
	private int mIndexInLeaf;
	private long mTransaction;


	public BTreeIterator(BTree aTree)
	{
		mTree = aTree;
		mTransaction = -1;
	}


	@Override
	public boolean hasNext()
	{
		if (!mClosed && mNext == null)
		{
			mNext = next();
		}
		return mNext != null;
	}


	@Override
	public Document next()
	{
		if (mClosed)
		{
			throw new IllegalStateException();
		}

		if (mNext == null)
		{
			if (mTransaction != mTree.getUpdateCounter())
			{
				mLeafNode = mTree.findLeaf(mLastKey);
				mIndexInLeaf = mLastKey == null ? 0 : mLeafNode.mMap.findEntryAfter(mLastKey);
			}

			if (mIndexInLeaf >= mLeafNode.size() && !advance())
			{
				mClosed = true;
				return null;
			}

			OpResult op = mLeafNode.mMap.get(mIndexInLeaf++);

			mTransaction = mTree.getUpdateCounter();
			mLastKey = op.entry.getKey();
			mNext = unmarshal(op.entry);
		}

		Document tmp = mNext;
		mNext = null;
		return tmp;
	}


	private boolean advance()
	{
		BTreeInteriorNode parent = mLeafNode.mParent;
		BTreeNode child = mLeafNode;

		for (;;)
		{
			if (parent == null)
			{
				return false;
			}

			int index = parent.indexOf(child) + 1;

			if (index > 0 && index < parent.size()) // was >=
			{
				child = parent.getChild(index);
				break;
			}

			child = parent;
			parent = parent.mParent;
		}

		while (child instanceof BTreeInteriorNode v)
		{
			child = v.getChild(0);
		}

		mLeafNode = (BTreeLeafNode)child;
		mIndexInLeaf = 0;

		return true;
	}


	@Override
	public void remove()
	{
		if (mLastKey != null)
		{
			remove(mLastKey);
		}
	}


	protected Document unmarshal(ArrayMapEntry aEntry)
	{
		return aEntry.getValue();
	}


	protected void remove(ArrayMapKey aKey)
	{
		mTree.remove(aKey);
	}
}
