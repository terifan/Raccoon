package org.terifan.raccoon.io.secure;

import org.terifan.security.random.ISAAC;


public class BlockKeyGenerator
{
	private final static ISAAC PRNG = new ISAAC();


	public static int[] generate()
	{
		return new int[]{
			PRNG.nextInt(),
			PRNG.nextInt(),
			PRNG.nextInt(),
			PRNG.nextInt(),
			PRNG.nextInt(),
			PRNG.nextInt(),
			PRNG.nextInt(),
			PRNG.nextInt()
		};
	}
}
