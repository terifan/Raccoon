package org.terifan.raccoon;

import java.util.Iterator;
import org.terifan.raccoon.document.Document;
import org.terifan.raccoon.util.Log;


public final class DocumentIterator implements Iterator<Document>
{
	private RaccoonCollection mCollection;
	private BTreeEntryIterator mBTreeEntryIterator;


	public DocumentIterator(RaccoonCollection aCollection)
	{
		mCollection = aCollection;
		mBTreeEntryIterator = new BTreeEntryIterator(aCollection._getImplementation());
	}


	public void setRange(ArrayMapKey aLow, ArrayMapKey aHigh)
	{
		mBTreeEntryIterator.setRange(aLow, aHigh);
	}


	@Override
	public boolean hasNext()
	{
		boolean hasMore = mBTreeEntryIterator.hasNext();

		if (!hasMore)
		{
			mCollection = null;
			mBTreeEntryIterator = null;
		}

		return hasMore;
	}


	@Override
	public Document next()
	{
		Log.d("next entity");
		Log.inc();

		try
		{
			return mCollection.unmarshalDocument(mBTreeEntryIterator.next(), new Document());
		}
		finally
		{
			Log.dec();
		}
	}


	@Override
	public void remove()
	{
		mBTreeEntryIterator.remove();
	}
}
