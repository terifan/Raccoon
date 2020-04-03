package org.terifan.raccoon;


public class CommitBlockedException extends RuntimeException
{
	private static final long serialVersionUID = 1L;


	public CommitBlockedException(String aMessage)
	{
		super(aMessage);
	}
}
