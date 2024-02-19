package org.terifan.raccoon.util;


public class AssertionException extends RuntimeException
{
	public AssertionException(String aMessage)
	{
		super(aMessage);
	}


	public AssertionException(String aMessage, Object... aArguments)
	{
		super(String.format(aMessage, aArguments));
	}
}
