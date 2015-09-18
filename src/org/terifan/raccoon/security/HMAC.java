package org.terifan.raccoon.security;

import java.security.MessageDigest;
import java.util.Arrays;


public final class HMAC extends MessageDigest implements Cloneable
{
	private transient MessageDigest mMessageDigest;
	private transient byte [] mInputPad;
	private transient byte [] mOutputPad;


	private HMAC(MessageDigest aMessageDigest)
	{
		super("HMAC-"+aMessageDigest.getAlgorithm());

		mMessageDigest = aMessageDigest;
	}


	public HMAC(MessageDigest aMessageDigest, byte [] aPassword)
	{
		this(aMessageDigest);

		init(aPassword);
	}


	private void init(byte [] aPassword)
	{
		mMessageDigest.reset();

		int blockLength;
		if (mMessageDigest instanceof SHA512)
		{
			blockLength = 128;
		}
		else
		{
			blockLength = 64;
		}

		if (aPassword.length > blockLength)
		{
			aPassword = mMessageDigest.digest(aPassword);
		}

		mInputPad = new byte[blockLength];

		System.arraycopy(aPassword, 0, mInputPad, 0, aPassword.length);

		mOutputPad = mInputPad.clone();

		for (int i = 0; i < mInputPad.length; i++)
		{
			mInputPad[i] ^= 0x36;
			mOutputPad[i] ^= 0x5c;
		}

		engineReset();
	}


	public MessageDigest getMessageDigest()
	{
		return mMessageDigest;
	}


	@Override
	protected byte [] engineDigest()
	{
		byte [] tmp = mMessageDigest.digest();
		mMessageDigest.update(mOutputPad);
		byte [] out = mMessageDigest.digest(tmp);

		engineReset();

		return out;
	}


	@Override
	protected int engineGetDigestLength()
	{
		return mMessageDigest.getDigestLength();
	}


	@Override
	protected void engineReset()
	{
		mMessageDigest.reset();
		mMessageDigest.update(mInputPad);
	}


	@Override
	protected void engineUpdate(byte aBuffer)
	{
		mMessageDigest.update(aBuffer);
	}


	@Override
	protected void engineUpdate(byte [] aBuffer, int aOffset, int aLength)
	{
		mMessageDigest.update(aBuffer, aOffset, aLength);
	}


	@Override
	public HMAC clone() throws CloneNotSupportedException
	{
		HMAC h = new HMAC((MessageDigest)mMessageDigest.clone());
		h.mInputPad = mInputPad.clone();
		h.mOutputPad = mOutputPad.clone();
		return h;
	}


	@Override
	public String toString()
	{
		return "HMAC-" + mMessageDigest.toString();
	}


	@Override
	public void reset()
	{
		mMessageDigest.reset();
		Arrays.fill(mInputPad, (byte)0);
		Arrays.fill(mOutputPad, (byte)0);
		super.reset();
	}
}