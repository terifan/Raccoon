package org.terifan.raccoon.io;


public class DatabaseIOException extends RuntimeException
{
	private static final long serialVersionUID = 1L;


	public DatabaseIOException()
	{
	}


	public DatabaseIOException(String aMessage)
	{
		super(aMessage);
	}


	public DatabaseIOException(String aMessage, Throwable aCause)
	{
		super(aMessage, aCause);
	}


	public DatabaseIOException(Throwable aCause)
	{
		super(aCause);
	}
}
