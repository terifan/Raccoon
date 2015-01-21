package org.terifan.raccoon.io;

import java.io.IOException;


public class UnsupportedVersionException extends IOException
{
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
