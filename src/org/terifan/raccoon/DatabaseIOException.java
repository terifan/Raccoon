package org.terifan.raccoon;


public class DatabaseIOException extends RuntimeException
{
	private static final long serialVersionUID = 1L;


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
