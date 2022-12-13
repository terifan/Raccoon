package org.terifan.raccoon;

import java.util.Iterator;
import org.terifan.bundle.Document;
import org.terifan.raccoon.btree.ArrayMapEntry;
import org.terifan.raccoon.util.Log;


final class DocumentIterator implements Iterator<Document>
{
	private final Iterator<ArrayMapEntry> mIterator;


	public DocumentIterator(Iterator<ArrayMapEntry> aIterator)
	{
		mIterator = aIterator;
	}


	@Override
	public boolean hasNext()
	{
		return mIterator.hasNext();
	}


	@Override
	public Document next()
	{
		Log.d("next entity");
		Log.inc();

		try
		{
			return Document.unmarshal(mIterator.next().getValue());
		}
		finally
		{
			Log.dec();
		}
	}


	@Override
	public void remove()
	{
		mIterator.remove();
	}
}
