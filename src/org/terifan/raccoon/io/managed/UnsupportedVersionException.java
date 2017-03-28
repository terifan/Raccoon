package org.terifan.raccoon.io.managed;

import java.io.IOException;


public class UnsupportedVersionException extends IOException
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
