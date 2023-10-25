package org.terifan.raccoon;


public class EntryNotFoundException extends DatabaseException
{
	private static final long serialVersionUID = 1L;


	public EntryNotFoundException(Object aKey)
	{
		super(aKey == null ? null : aKey.toString());
	}
}
