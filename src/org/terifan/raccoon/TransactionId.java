package org.terifan.raccoon;


public class TransactionId 
{
	private long mTransaction;


	public TransactionId(long aTransaction)
	{
		mTransaction = aTransaction;
	}
	
	
	public long get()
	{
		return mTransaction;
	}
}
