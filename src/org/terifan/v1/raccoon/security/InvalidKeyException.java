package org.terifan.v1.raccoon.security;


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