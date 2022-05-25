package org.terifan.raccoon.io.secure;

import org.terifan.security.cryptography.ISAAC;


public class BlockKeyGenerator
{
	public static long[] generate()
	{
		return new long[]{ISAAC.PRNG.nextLong(), ISAAC.PRNG.nextLong(), ISAAC.PRNG.nextLong(), ISAAC.PRNG.nextLong()};
	}
}
