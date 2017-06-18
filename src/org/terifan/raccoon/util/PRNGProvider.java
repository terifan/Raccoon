package org.terifan.raccoon.util;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;


public final class PRNGProvider 
{
	public static SecureRandom getInstance()
	{
		try
		{
			return SecureRandom.getInstanceStrong();
		}
		catch (NoSuchAlgorithmException e)
		{
			return new SecureRandom();
		}
	}
}
