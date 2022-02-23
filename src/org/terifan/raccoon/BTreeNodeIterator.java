package org.terifan.raccoon;

import java.util.Iterator;


public class BTreeNodeIterator implements Iterator<BTreeNode>
{
	private int mPosition;
	private BTreeNode mCurrent;


	BTreeNodeIterator(BTreeNode aRoot)
	{
		mCurrent = aRoot;
	}


	@Override
	public boolean hasNext()
	{
		if (mCurrent == null)
		{
//				if (mPosition == 0)
			{
				return false;
			}

//				mNode = null;
//				mPosition++;
		}

		return true;
	}


	@Override
	public BTreeNode next()
	{
		BTreeNode tmp = mCurrent;
		mCurrent = null;
		return tmp;
	}
}
