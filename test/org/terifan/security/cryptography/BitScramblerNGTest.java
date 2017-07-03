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

		scramble(0, scrambled);

		assertEquals(scrambled, new byte[]{(byte)0xdd,(byte)0xdc,(byte)0x99,(byte)0xb3,0x4c,(byte)0x8f,0x6b,0x48,(byte)0xff,(byte)0xbf,0x72,0x41,(byte)0xf7,(byte)0xae,(byte)0xff,(byte)0xa0});
	}
}
