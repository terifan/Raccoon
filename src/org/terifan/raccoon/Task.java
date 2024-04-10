package org.terifan.raccoon;

import org.terifan.logging.Logger;


abstract class Task implements Runnable
{
	private String mDescription;
	private RaccoonDatabase mInstance;


	public Task(RaccoonDatabase aInstance, String aDescription)
	{
		mInstance = aInstance;
		mDescription = aDescription;
	}


	public abstract void call();


	@Override
	public void run()
	{
//		mInstance.log.d(mDescription);
//		mInstance.log.inc();

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
//			mInstance.log.dec();
		}
	}
}
