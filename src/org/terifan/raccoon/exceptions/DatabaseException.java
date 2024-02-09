package org.terifan.raccoon.exceptions;


public class DatabaseException extends RuntimeException
{
	private final static long serialVersionUID = 1L;


	public DatabaseException()
	{
	}


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
