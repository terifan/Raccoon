package org.terifan.raccoon;

import java.util.concurrent.atomic.AtomicLong;


public final class TransactionCounter
{
	private AtomicLong mCounter;


	TransactionCounter()
	{
		mCounter = new AtomicLong(0);
	}


	TransactionCounter(long aCounter)
	{
		mCounter = new AtomicLong(aCounter);
	}


	public long get()
	{
		return mCounter.get();
	}


	void increment()
	{
		mCounter.incrementAndGet();
	}


	void set(long aCounter)
	{
		mCounter.set(aCounter);
	}
}
