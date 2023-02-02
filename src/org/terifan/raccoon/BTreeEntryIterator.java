package org.terifan.raccoon;


public class BTreeEntryIterator extends Sequence<ArrayMapEntry>
{
	private BTreeNodeIterator mOuterSequence;
	private ArrayMapEntryIterator mInnerSequence;


	public BTreeEntryIterator(BTree aTree, Query aQuery)
	{
		mOuterSequence = new BTreeNodeIterator(aTree, aQuery);
	}


	@Override
	public ArrayMapEntry advance()
	{
		for (;;)
		{
			if (mInnerSequence == null)
			{
				if (!mOuterSequence.hasNext())
				{
					mOuterSequence = null;
					return null;
				}

				mInnerSequence = mOuterSequence.next().mMap.iterator();
			}

			if (!mInnerSequence.hasNext())
			{
				mInnerSequence = null;
			}
			else
			{
				return mInnerSequence.next();
			}
		}
	}
}
