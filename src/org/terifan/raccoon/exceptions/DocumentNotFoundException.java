package org.terifan.raccoon.exceptions;


public class DocumentNotFoundException extends DatabaseException
{
	private static final long serialVersionUID = 1L;


	public DocumentNotFoundException()
	{
	}


	public DocumentNotFoundException(Object aKey)
	{
		super(aKey == null ? null : aKey.toString());
	}
}
