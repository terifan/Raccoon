package org.terifan.raccoon.io.secure;

import org.terifan.security.cryptography.ISAAC;


public class BlockKeyGenerator
{
	private final static ISAAC PRNG = new ISAAC();


	public static long[] generate()
	{
		return new long[]{PRNG.nextLong(), PRNG.nextLong(), PRNG.nextLong(), PRNG.nextLong()};
	}
}
