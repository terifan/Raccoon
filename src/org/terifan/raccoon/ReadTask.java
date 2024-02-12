package org.terifan.raccoon;

import org.terifan.raccoon.util.ReadWriteLock;


public abstract class ReadTask implements Runnable
{
	private String mLog;
	private RaccoonCollection mCollection;


	public ReadTask(RaccoonCollection aCollection, String aLog)
	{
		mCollection = aCollection;
		mLog = aLog;
	}


	public abstract void call();


	@Override
	public void run()
	{
		mCollection.log.i(mLog);
		mCollection.log.inc();

		try (ReadWriteLock.ReadLock lock = mCollection.mLock.readLock())
		{
			call();
		}
		finally
		{
			mCollection.log.dec();
		}
	}
}
