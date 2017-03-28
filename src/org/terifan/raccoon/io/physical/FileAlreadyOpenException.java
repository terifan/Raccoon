package org.terifan.raccoon.io.physical;

import java.io.IOException;


public class FileAlreadyOpenException extends IOException
{
	private static final long serialVersionUID = 1L;


	public FileAlreadyOpenException(String aMessage, Throwable aCause)
	{
		super(aMessage, aCause);
	}
}
