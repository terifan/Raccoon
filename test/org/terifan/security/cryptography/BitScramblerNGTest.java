package org.terifan.security.cryptography;

import java.util.Random;
import static org.terifan.security.cryptography.BitScrambler.scramble;
import static org.terifan.security.cryptography.BitScrambler.unscramble;
import static org.testng.Assert.*;
import org.testng.annotations.Test;


public class BitScramblerNGTest
{
	@Test
	public void testScrambling()
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


	@Test
	public void testFailure()
	{
		byte[] plaintText = new byte[256];

		Random random = new java.util.Random();
		random.nextBytes(plaintText);

		byte[] scrambled = plaintText.clone();

		scramble(0, scrambled);

		byte[] unscrambled = scrambled.clone();

		unscramble(1, unscrambled);

		assertNotEquals(scrambled, plaintText);
		assertNotEquals(unscrambled, plaintText);
	}


	@Test
	public void testExpectedOutput()
	{
		byte[] scrambled = new byte[16];
		for (int i = 0; i < scrambled.length; i++)
		{
			scrambled[i] = (byte)i;
		}

		scramble(0, scrambled);

		assertEquals(scrambled, new byte[]{(byte)0x00,(byte)0xa8,(byte)0x20,(byte)0x82,(byte)0x7,(byte)0x21,(byte)0x22,(byte)0x88,(byte)0x24,(byte)0x19,(byte)0x9,(byte)0x82,(byte)0xc,(byte)0x50,(byte)0x64,(byte)0x20});
	}
}
