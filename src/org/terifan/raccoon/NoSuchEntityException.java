package org.terifan.raccoon;


public class NoSuchEntityException extends RuntimeException
{
	public NoSuchEntityException(String aMessage)
	{
		super(aMessage);
	}
}
