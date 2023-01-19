package org.terifan.raccoon;

import org.terifan.raccoon.document.Document;


public class IdentityCounter
{
	private final Document mConfiguration;


	public IdentityCounter(Document aConfiguration)
	{
		mConfiguration = aConfiguration;
	}


	public long get()
	{
		return mConfiguration.get("identityCounter", 1L);
	}


	public void set(long aValue)
	{
		mConfiguration.put("identityCounter", aValue);
	}


	synchronized long next()
	{
		long value = mConfiguration.get("identityCounter", 0L) + 1L;
		mConfiguration.put("identityCounter", value);
		return value;
	}


	@Override
	public String toString()
	{
		return "" + get();
	}
}
