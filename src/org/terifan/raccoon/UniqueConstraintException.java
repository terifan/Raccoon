package org.terifan.raccoon;


public class UniqueConstraintException extends DatabaseException
{
	private static final long serialVersionUID = 1L;


	public UniqueConstraintException()
	{
	}


	public UniqueConstraintException(String aMessage)
	{
		super(aMessage);
	}


	public UniqueConstraintException(String aMessage, Throwable aCause)
	{
		super(aMessage, aCause);
	}


	public UniqueConstraintException(Throwable aCause)
	{
		super(aCause);
	}
}
