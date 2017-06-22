package org.terifan.security.cryptography;

import java.util.Random;
import org.terifan.raccoon.util.Log;
import static org.terifan.security.cryptography.BitScrambler.scramble;
import static org.terifan.security.cryptography.BitScrambler.unscramble;
import static org.testng.Assert.*;
import org.testng.annotations.Test;


public class BitScramblerNGTest
{
	@Test
	public void test()
	{
		byte[] plaintText = new byte[256];

		Random random = new java.util.Random();
		random.nextBytes(plaintText);

		long key = random.nextLong();

		byte[] scrambled = plaintText.clone();

		scramble(key, scrambled);

		byte[] unscrambled = scrambled.clone();

		unscramble(key, unscrambled);

//		Log.hexDump(plaintText);
//		System.out.println();
//		Log.hexDump(scrambled);
//		System.out.println();
//		Log.hexDump(unscrambled);

		assertNotEquals(scrambled, plaintText);
		assertEquals(unscrambled, plaintText);
	}
}
