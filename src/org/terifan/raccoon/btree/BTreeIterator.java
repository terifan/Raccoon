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


	protected void check(BTreeNode aNode)
	{
		if (aNode instanceof BTreeInteriorNode v)
		{
			System.out.println(".. ".repeat(5-aNode.mLevel) + "node");
			for (int i = 0; i < v.size(); i++)
			{
				BTreeNode child = v.getNode(i);
				if (child.mParent != aNode)
				{
					throw new IllegalStateException();
				}
				check(child);
			}
		}
		else
		{
			System.out.println(".. ".repeat(5-aNode.mLevel) + "leaf");
		}
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

//			if (mLeafNode.mMap.size() == 0)
//			{
//				check(mTree._root());
//			}

			ArrayMapEntry entry = mLeafNode.mMap.get(mIndexInLeaf++, new ArrayMapEntry());

			mTransaction = mTree.getUpdateCounter();
			mLastKey = entry.getKey();
			mNext = unmarshal(entry);
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
			remove(new ArrayMapEntry(mLastKey));
		}
	}


	protected Document unmarshal(ArrayMapEntry aEntry)
	{
		return aEntry.getValue();
	}


	protected void remove(ArrayMapEntry aEntry)
	{
		mTree.remove(aEntry);
	}
}
