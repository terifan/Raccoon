package org.terifan.security.messagedigest;

import java.security.DigestException;


/**
 * FIPS-180-2 compliant SHA-512 implementation<p>
 *
 * Implementation from bouncycastle.org
 *
 * Copyright (c) 2000-2006 The Legion Of The Bouncy Castle (http://www.bouncycastle.org)
 */
public final class SHA512 extends SHABase implements Cloneable
{
	private final static int DIGEST_LENGTH = 64;


	public SHA512()
	{
		super("sha-512");

		engineReset();
	}


	/**
	 * Constructor for cloning
	 */
	public SHA512(SHA512 aBase)
	{
		super("sha-512", aBase);
	}


	@Override
	protected int engineDigest(byte[] out, int outOff, int len) throws DigestException
	{
		if (out.length - outOff < DIGEST_LENGTH)
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
		unpackWord(H7, out, outOff + 48);
		unpackWord(H8, out, outOff + 56);

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

		/* SHA-512 initial hash value
         * The first 64 bits of the fractional parts of the square roots
         * of the first eight prime numbers
		 */
		H1 = 0x6a09e667f3bcc908L;
		H2 = 0xbb67ae8584caa73bL;
		H3 = 0x3c6ef372fe94f82bL;
		H4 = 0xa54ff53a5f1d36f1L;
		H5 = 0x510e527fade682d1L;
		H6 = 0x9b05688c2b3e6c1fL;
		H7 = 0x1f83d9abfb41bd6bL;
		H8 = 0x5be0cd19137e2179L;
	}


	@Override
	public String toString()
	{
		return "SHA512";
	}


	@Override
	public SHA512 clone()
	{
		return new SHA512(this);
	}
}
