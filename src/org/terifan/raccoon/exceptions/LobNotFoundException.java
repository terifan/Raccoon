package org.terifan.raccoon.exceptions;


public class LobNotFoundException extends DatabaseException
{
	private static final long serialVersionUID = 1L;


	public LobNotFoundException(Object aKey)
	{
		super(aKey == null ? null : aKey.toString());
	}
}
