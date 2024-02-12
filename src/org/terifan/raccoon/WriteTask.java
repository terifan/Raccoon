package org.terifan.raccoon;

import org.terifan.raccoon.util.ReadWriteLock;


public abstract class WriteTask implements Runnable
{
	private String mLog;
	private RaccoonCollection mCollection;


	public WriteTask(RaccoonCollection aCollection, String aLog)
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

		mCollection.mModCount++;

		try (ReadWriteLock.WriteLock lock = mCollection.mLock.writeLock())
		{
			call();
		}
		finally
		{
			mCollection.log.dec();
		}
	}
}
