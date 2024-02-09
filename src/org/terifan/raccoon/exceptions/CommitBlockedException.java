package org.terifan.raccoon.exceptions;


public class CommitBlockedException extends RuntimeException
{
	private static final long serialVersionUID = 1L;


	public CommitBlockedException(String aMessage)
	{
		super(aMessage);
	}
}
