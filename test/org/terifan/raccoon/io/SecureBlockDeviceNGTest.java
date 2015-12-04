package org.terifan.raccoon.io;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import org.terifan.raccoon.util.Log;
import org.testng.annotations.Test;
import static org.testng.Assert.*;


public class SecureBlockDeviceNGTest
{
	@Test
	public void testSomeMethod() throws IOException
	{
		int unitSize = 512;
		int numUnits = 8;
		int blocksPerUnit = 2;

		MemoryBlockDevice blockDevice = new MemoryBlockDevice(unitSize);

		Random rnd = new Random();

		byte[] input = new byte[numUnits * unitSize];
		byte[] output = new byte[numUnits * unitSize];
		rnd.nextBytes(input);

		long[] blockKeys = new long[numUnits];
		for (int i = 0; i < numUnits; i++)
		{
			blockKeys[i] = rnd.nextLong();
		}

		Arrays.fill(input, (byte)65);

		byte[] original = input.clone();

		try (SecureBlockDevice device = new SecureBlockDevice(blockDevice, new AccessCredentials("password", EncryptionFunction.AESTwofishSerpent, KeyGenerationFunction.Skein512)))
		{
			for (int i = 0; i < numUnits / blocksPerUnit; i++)
			{
				device.writeBlock(blocksPerUnit * i, input, blocksPerUnit * i * unitSize, blocksPerUnit * unitSize, blockKeys[i]);
			}
		}

		assertEquals(input, original);

		try (SecureBlockDevice device = new SecureBlockDevice(blockDevice, new AccessCredentials("password")))
		{
			for (int i = 0; i < numUnits / blocksPerUnit; i++)
			{
				device.readBlock(blocksPerUnit * i, output, blocksPerUnit * i * unitSize, blocksPerUnit * unitSize, blockKeys[i]);
			}
		}

		assertEquals(output, input);
	}
}
