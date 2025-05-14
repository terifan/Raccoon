package org.terifan.raccoon;

import org.terifan.raccoon.document.Document;


class RaccoonMap extends Document implements AutoCloseable
{
	private RaccoonDatabase mDatabase;
	private String mName;


	RaccoonMap(RaccoonDatabase aDatabase, String aName)
	{
		mDatabase = aDatabase;
		mName = aName;
	}


	public void beforePersist()
	{
	}


	public void drop()
	{
		mDatabase.deleteMapImpl(mName);
		mDatabase = null;
	}


	@Override
	public void close()
	{
	}
}
