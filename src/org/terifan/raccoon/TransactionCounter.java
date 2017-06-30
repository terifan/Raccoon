package org.terifan.raccoon;


public final class TransactionCounter
{
	private long mCounter;


	public TransactionCounter(long aCounter)
	{
		mCounter = aCounter;
	}


	public synchronized long get()
	{
		return mCounter;
	}


	synchronized void increment()
	{
		mCounter++;
	}


	public synchronized TransactionCounter set(long aCounter)
	{
		mCounter = aCounter;
		return this;
	}
}
