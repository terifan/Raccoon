package org.terifan.raccoon;


public class DatabaseClosedException extends IllegalStateException
{
	private static final long serialVersionUID = 1L;


	public DatabaseClosedException()
	{
	}


	public DatabaseClosedException(String aMessage)
	{
		super(aMessage);
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
