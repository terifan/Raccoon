package org.terifan.v1.raccoon;


public class DatabaseException extends RuntimeException
{
	public DatabaseException(String aMessage)
	{
		super(aMessage);
	}


	public DatabaseException(Throwable aCause)
	{
		super(aCause);
	}


	public DatabaseException(String aMessage, Throwable aCause)
	{
		super(aMessage, aCause);
	}
}
