package org.terifan.raccoon;

import org.terifan.raccoon.document.Document;


public class Query
{
	private final Document mParams;


	public Query(Document aParams)
	{
		mParams = aParams;
	}


	public Document getParams()
	{
		return mParams;
	}
}
