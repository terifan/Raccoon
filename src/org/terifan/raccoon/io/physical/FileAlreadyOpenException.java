package org.terifan.raccoon.io.physical;

import org.terifan.raccoon.DatabaseIOException;


public class FileAlreadyOpenException extends DatabaseIOException
{
	private static final long serialVersionUID = 1L;


	public FileAlreadyOpenException(String aMessage, Throwable aCause)
	{
		super(aMessage, aCause);
	}
}
