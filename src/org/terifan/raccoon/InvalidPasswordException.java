package org.terifan.raccoon;


public class InvalidPasswordException extends DatabaseException
{
	private static final long serialVersionUID = 1L;


	public InvalidPasswordException()
	{
	}


	public InvalidPasswordException(String aMessage)
	{
		super(aMessage);
	}
}
