package org.terifan.raccoon.io.managed;

import org.terifan.raccoon.DatabaseIOException;


public class UnsupportedVersionException extends DatabaseIOException
{
	private static final long serialVersionUID = 1L;


	public UnsupportedVersionException()
	{
	}


	public UnsupportedVersionException(String message)
	{
		super(message);
	}


	public UnsupportedVersionException(String message, Throwable cause)
	{
		super(message, cause);
	}


	public UnsupportedVersionException(Throwable cause)
	{
		super(cause);
	}
}
