package org.terifan.raccoon.security;

import java.util.Arrays;


public final class SecretKey
{
	private transient byte[] mKeyBytes;


	public SecretKey(byte[] aKeyBytes)
	{
		this(aKeyBytes, 0, aKeyBytes.length);
	}


	public SecretKey(byte[] aKeyBytes, int aOffset, int aLength)
	{
		mKeyBytes = Arrays.copyOfRange(aKeyBytes, aOffset, aOffset + aLength);
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