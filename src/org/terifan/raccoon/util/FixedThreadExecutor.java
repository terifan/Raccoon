package org.terifan.raccoon.util;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


public class FixedThreadExecutor implements AutoCloseable
{
	private ExecutorService mExecutorService;
	private Exception mException;


	public FixedThreadExecutor(int aThreads)
	{
		mExecutorService = Executors.newFixedThreadPool(aThreads);
	}


	public void submit(Task aTask)
	{
		mExecutorService.submit(() ->
		{
			try
			{
				aTask.run();
			}
			catch (Exception e)
			{
				mException = e;
			}
		});
	}


	@Override
	public void close() throws Exception
	{
		if (mExecutorService != null)
		{
			try
			{
				mExecutorService.shutdown();
				mExecutorService.awaitTermination(1, TimeUnit.HOURS);
			}
			catch (InterruptedException e)
			{
			}
			finally
			{
				mExecutorService = null;
			}

			if (mException != null)
			{
				throw mException;
			}
		}
	}


	@FunctionalInterface
	public interface Task<V>
	{
		void run() throws Exception;
	}


	public static void main(String... args)
	{
		try
		{
			try (FixedThreadExecutor executor = new FixedThreadExecutor(2))
			{
				executor.submit(() -> Thread.sleep(1000));
				executor.submit(() -> Thread.sleep(500));
			}
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
	}
}
