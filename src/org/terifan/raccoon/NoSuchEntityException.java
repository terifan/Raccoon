package org.terifan.raccoon;


public class NoSuchEntityException extends RuntimeException
{
	private static final long serialVersionUID = 1L;


	public NoSuchEntityException(String aMessage)
	{
		super(aMessage);
	}
}
