package org.terifan.raccoon;


public final class TransactionGroup
{
	private long mTransactionId;


	public TransactionGroup(long aTransactionId)
	{
		mTransactionId = aTransactionId;
	}


	public synchronized long get()
	{
		return mTransactionId;
	}
}
