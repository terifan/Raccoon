package org.terifan.raccoon.security;

import java.util.Random;
import org.testng.annotations.Test;
import org.testng.annotations.DataProvider;
import static org.testng.Assert.*;


public class ElephantNGTest
{
	@Test(dataProvider = "Data-Provider-Function")
	public void testSimpleReadWrite(int aUnitSize) throws Exception
	{
		Elephant instance = new Elephant(aUnitSize);

		byte[] cipherKey = new byte[32];
		byte[] tweakKey = new byte[32];

		Random rnd = new Random(1);
		rnd.nextBytes(cipherKey);
		rnd.nextBytes(tweakKey);

		Cipher cipher = new Twofish(new SecretKey(cipherKey));
		Cipher tweakCipher = new Twofish(new SecretKey(tweakKey));

		int[] iv = new int[4];
		int[] tweak = new int[8];

		long extraTweak = rnd.nextLong();
		long unitIndex = rnd.nextLong() & Long.MAX_VALUE;
		iv[0] = rnd.nextInt();
		iv[1] = rnd.nextInt();
		iv[2] = rnd.nextInt();
		iv[3] = rnd.nextInt();
		tweak[0] = rnd.nextInt();
		tweak[1] = rnd.nextInt();
		tweak[2] = rnd.nextInt();
		tweak[3] = rnd.nextInt();
		tweak[4] = rnd.nextInt();
		tweak[5] = rnd.nextInt();
		tweak[6] = rnd.nextInt();
		tweak[7] = rnd.nextInt();

		byte[] clearText = new byte[3 * aUnitSize];

		byte[] encoded = clearText.clone();
		instance.encrypt(encoded, 0, 3 * aUnitSize, unitIndex + 0, iv, cipher, tweakCipher, tweak, extraTweak);

		byte[] decoded = encoded.clone();
		instance.decrypt(decoded, 0, 1 * aUnitSize, unitIndex + 0, iv, cipher, tweakCipher, tweak, extraTweak);
		instance.decrypt(decoded, aUnitSize, 2 * aUnitSize, unitIndex + 1, iv, cipher, tweakCipher, tweak, extraTweak);

		assertEquals(clearText, decoded);
	}


	@DataProvider(name = "Data-Provider-Function")
	public Object[][] data()
	{
		return new Object[][]{
			{128},
			{512},
			{4096},
			{65536}
		};
	}
}
