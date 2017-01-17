package org.terifan.raccoon;


public class DatabaseClosedException extends IllegalStateException
{
	public DatabaseClosedException()
	{
	}


	public DatabaseClosedException(String aS)
	{
		super(aS);
	}


	public DatabaseClosedException(String aMessage, Throwable aCause)
	{
		super(aMessage, aCause);
	}


	public DatabaseClosedException(Throwable aCause)
	{
		super(aCause);
	}
}
