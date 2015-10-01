package org.terifan.raccoon.security;

import java.util.Arrays;


public final class SecretKey
{
	private transient final byte[] mKeyBytes;


	public SecretKey(byte[] aKeyBytes)
	{
		mKeyBytes = aKeyBytes.clone();
	}


	byte[] bytes()
	{
		return mKeyBytes;
	}


	public void reset()
	{
		Arrays.fill(mKeyBytes, (byte)0);
	}
}