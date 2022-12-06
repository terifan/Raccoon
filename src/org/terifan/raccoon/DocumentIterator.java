package org.terifan.raccoon;

import java.util.Iterator;
import org.terifan.bundle.Document;
import org.terifan.raccoon.btree.ArrayMapEntry;
import org.terifan.raccoon.util.Log;


final class DocumentIterator implements Iterator<Document>
{
	private final Iterator<ArrayMapEntry> mIterator;
	private final Database mDatabase;


	DocumentIterator(Database aDatabase, TableInstance aTable, Iterator<ArrayMapEntry> aIterator)
	{
		mDatabase = aDatabase;
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

		ArrayMapEntry entry = mIterator.next();
		Document document = Document.unmarshal(entry.getValue());

		Log.dec();

		return document;
	}


	@Override
	public void remove()
	{
		mIterator.remove();
	}
}
