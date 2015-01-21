package org.terifan.raccoon.security;


public class InvalidKeyException extends RuntimeException
{
	public InvalidKeyException()
	{
	}

	public InvalidKeyException(String aMessage)
	{
		super(aMessage);
	}
}