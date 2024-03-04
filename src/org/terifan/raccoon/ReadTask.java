package org.terifan.raccoon;

import org.terifan.logging.Logger;



abstract class ReadTask implements Runnable
{
	private String mDescription;
	private RaccoonCollection mCollection;


	public ReadTask(RaccoonCollection aCollection, String aDescription)
	{
		mCollection = aCollection;
		mDescription = aDescription;
	}


	public abstract void call();


	@Override
	public void run()
	{
		mCollection.log.i(mDescription);
		mCollection.log.inc();

		try
		{
			call();
		}
		catch (Exception | Error e)
		{
			Logger.getLogger().e("{}", e);
		}
		finally
		{
			mCollection.log.dec();
		}
	}
}
