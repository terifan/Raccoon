package org.terifan.raccoon;

import java.util.Iterator;
import org.terifan.bundle.Document;
import org.terifan.raccoon.btree.BTreeEntryIterator;
import org.terifan.raccoon.util.Log;


final public class DocumentIterator implements Iterator<Document>
{
	private final BTreeEntryIterator mEntryIterator;


	public DocumentIterator(BTreeEntryIterator aIterator)
	{
		mEntryIterator = aIterator;
	}


	@Override
	public boolean hasNext()
	{
		return mEntryIterator.hasNext();
	}


	@Override
	public Document next()
	{
		Log.d("next entity");
		Log.inc();

		try
		{
			return Document.unmarshal(mEntryIterator.next().getValue());
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
