package org.terifan.raccoon.io;

import java.io.IOException;
import java.util.Random;
import org.testng.annotations.Test;
import org.testng.annotations.DataProvider;
import static org.testng.Assert.*;


public class SecureBlockDeviceTest
{
	@Test
	public void testSomeMethod() throws IOException
	{
		int S = 512;
		int L = 1000;

		MemoryBlockDevice blockDevice = new MemoryBlockDevice(S);

		Random rnd = new Random();

		byte[] output = new byte[L * S];
		byte[] input = new byte[L * S];
		rnd.nextBytes(output);

		long[] keys = new long[L];
		for (int i = 0; i < L; i++)
		{
			keys[i] = rnd.nextLong();
		}

		try (SecureBlockDevice device = new SecureBlockDevice(blockDevice, new AccessCredentials("password", EncryptionFunction.AESTwofishSerpent, KeyGenerationFunction.Skein512)))
		{
			for (int i = 0; i < L; i++)
			{
				device.writeBlock(i, output, i * S, S, keys[i]);
			}
		}

		try (SecureBlockDevice device = new SecureBlockDevice(blockDevice, new AccessCredentials("password")))
		{
			for (int i = 0; i < L; i++)
			{
				device.readBlock(i, input, i * S, S, keys[i]);
			}
		}

		assertEquals(input, output);
	}
}
