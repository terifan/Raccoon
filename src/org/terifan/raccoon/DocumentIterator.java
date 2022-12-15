package org.terifan.raccoon;

import java.util.Iterator;
import org.terifan.bundle.Document;
import org.terifan.raccoon.util.Log;


public final class DocumentIterator implements Iterator<Document>
{
	private RaccoonCollection mCollection;
	private BTreeEntryIterator mEntryIterator;


	public DocumentIterator(RaccoonCollection aCollection)
	{
		mCollection = aCollection;
		mEntryIterator = new BTreeEntryIterator(aCollection.getImplementation());
	}


	@Override
	public boolean hasNext()
	{
		boolean hasMore = mEntryIterator.hasNext();

		if (!hasMore)
		{
			mCollection = null;
			mEntryIterator = null;
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
			return mCollection.unmarshalDocument(mEntryIterator.next());
		}
		finally
		{
			Log.dec();
		}
	}


	@Override
	public void remove()
	{
		mEntryIterator.remove();
	}
}
