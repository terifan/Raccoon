package org.terifan.v1.security;

import java.security.DigestException;


/**
 * FIPS-180-2 compliant SHA-384 implementation<p>
 *
 * Implementation from bouncycastle.org
 *
 * Copyright (c) 2000-2006 The Legion Of The Bouncy Castle (http://www.bouncycastle.org)
 */
public final class SHA384 extends SHABase implements Cloneable
{
    private final static int DIGEST_LENGTH = 48;


	public SHA384()
	{
		super("sha-384");

		engineReset();
	}

    /**
     * Constructor for cloning
     */
    public SHA384(SHA384 aBase)
    {
    	super("sha-384", aBase);
    }


	@Override
	protected int engineDigest(byte[] out, int outOff, int len) throws DigestException
	{
		if (out.length-outOff < DIGEST_LENGTH)
		{
			throw new DigestException("Buffer too short.");
		}

        finish();

        unpackWord(H1, out, outOff);
        unpackWord(H2, out, outOff + 8);
        unpackWord(H3, out, outOff + 16);
        unpackWord(H4, out, outOff + 24);
        unpackWord(H5, out, outOff + 32);
        unpackWord(H6, out, outOff + 40);

        reset();

		return DIGEST_LENGTH;
	}


	@Override
	protected int engineGetDigestLength()
	{
		return DIGEST_LENGTH;
	}


	@Override
	protected void engineReset()
	{
        baseReset();

        /* SHA-384 initial hash value
         * The first 64 bits of the fractional parts of the square roots
         * of the 9th through 16th prime numbers
         */
        H1 = 0xcbbb9d5dc1059ed8l;
        H2 = 0x629a292a367cd507l;
        H3 = 0x9159015a3070dd17l;
        H4 = 0x152fecd8f70e5939l;
        H5 = 0x67332667ffc00b31l;
        H6 = 0x8eb44a8768581511l;
        H7 = 0xdb0c2e0d64f98fa7l;
        H8 = 0x47b5481dbefa4fa4l;
	}


	@Override
	public String toString()
	{
		return "SHA384";
	}


	@Override
	public SHA384 clone()
	{
		return new SHA384(this);
	}
}