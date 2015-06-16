package tests;

import java.lang.management.ManagementFactory;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


public class __FixedThreadExecutor implements AutoCloseable
{
	private ExecutorService mExecutorService;
	private int mThreads;


	public __FixedThreadExecutor(int aThreads)
	{
		mThreads = aThreads;
	}


	/**
	 *
	 * @param aThreads
	 *   number of threads expressed as a number between 0 and 1 out of total available CPUs
	 */
	public __FixedThreadExecutor(float aThreads)
	{
		int cpu = ManagementFactory.getOperatingSystemMXBean().getAvailableProcessors();

		mThreads = Math.max(1, Math.min(cpu, (int)Math.round(cpu * (aThreads - (int)(aThreads - 0.000001)))));
	}


	public void submit(Runnable aRunnable)
	{
		init().submit(aRunnable);
	}


	public void submit(Runnable... aRunnables)
	{
		for (Runnable r : aRunnables)
		{
			init().submit(r);
		}
	}


	public void submit(Iterable<? extends Runnable> aRunnables)
	{
		for (Runnable r : aRunnables)
		{
			init().submit(r);
		}
	}


	@Override
	public void close()
	{
		close(Long.MAX_VALUE);
	}


	private synchronized boolean close(long aWaitMillis)
	{
		if (mExecutorService != null)
		{
			try
			{
				mExecutorService.shutdown();

				mExecutorService.awaitTermination(aWaitMillis, TimeUnit.MILLISECONDS);

				return false;
			}
			catch (InterruptedException e)
			{
				return true;
			}
			finally
			{
				mExecutorService = null;
			}
		}

		return false;
	}


	private synchronized ExecutorService init()
	{
		if (mExecutorService == null)
		{
			mExecutorService = Executors.newFixedThreadPool(mThreads);
		}

		return mExecutorService;
	}
}
