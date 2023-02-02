package org.terifan.raccoon;

import org.terifan.raccoon.document.Document;


public final class DocumentIterator extends Sequence<Document>
{
	private RaccoonCollection mCollection;
	private BTreeEntryIterator mBTreeEntryIterator;


	public DocumentIterator(RaccoonCollection aCollection, Query aQuery)
	{
		mCollection = aCollection;
		mBTreeEntryIterator = new BTreeEntryIterator(aCollection._getImplementation(), aQuery);
	}


	@Override
	protected Document advance()
	{
		if (mBTreeEntryIterator.hasNext())
		{
			return mCollection.unmarshalDocument(mBTreeEntryIterator.next(), new Document());
		}

		mCollection = null;
		mBTreeEntryIterator = null;
		return null;
	}
}
