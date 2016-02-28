package org.terifan.raccoon;


public class DatabaseIOException extends RuntimeException
{
	public DatabaseIOException(String aMessage)
	{
		super(aMessage);
	}


	public DatabaseIOException(Throwable aCause)
	{
		super(aCause);
	}


	public DatabaseIOException(String aMessage, Throwable aCause)
	{
		super(aMessage, aCause);
	}
}
