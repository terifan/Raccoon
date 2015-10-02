package org.terifan.raccoon;

import java.util.concurrent.atomic.AtomicLong;


public class TransactionId
{
	private AtomicLong mTransaction;


	TransactionId()
	{
		mTransaction = new AtomicLong(0);
	}


	public TransactionId(long aTransaction)
	{
		mTransaction = new AtomicLong(aTransaction);
	}


	public long get()
	{
		return mTransaction.get();
	}


	void increment()
	{
		mTransaction.incrementAndGet();
	}


	void set(long aTransactionId)
	{
		mTransaction.set(aTransactionId);
	}
}
