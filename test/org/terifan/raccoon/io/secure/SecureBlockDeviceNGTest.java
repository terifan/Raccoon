package org.terifan.raccoon.io.secure;

import org.terifan.raccoon.io.physical.MemoryBlockDevice;
import java.io.IOException;
import java.util.Random;
import org.terifan.raccoon.OpenOption;
import org.testng.annotations.Test;
import static org.testng.Assert.*;


public class SecureBlockDeviceNGTest
{
	@Test
	public void testLoadBootBlock() throws IOException
	{
		MemoryBlockDevice blockDevice = new MemoryBlockDevice(4096);
		AccessCredentials accessCredentials = new AccessCredentials("password").setIterationCount(100);

		try (SecureBlockDevice device = SecureBlockDevice.create(accessCredentials, blockDevice))
		{
			device.writeBlock(0, new byte[4096], 0, 4096, new long[2]);
		}

		assertEquals(blockDevice.length(), 3);

		try (SecureBlockDevice device = SecureBlockDevice.open(blockDevice, accessCredentials))
		{
			device.validateBootBlocks(accessCredentials);
		}
	}


	@Test
	public void testKeyAndEncryptionOptions() throws IOException
	{
		for (KeyGenerationFunction kgf : KeyGenerationFunction.values())
		{
			for (EncryptionFunction ef : EncryptionFunction.values())
			{
				for (CipherModeFunction cmf : CipherModeFunction.values())
				{
					Random rnd = new Random();

					int unitSize = 512;
					int numUnits = 32;
					int blocksPerUnit = 4;

					MemoryBlockDevice blockDevice = new MemoryBlockDevice(unitSize);

					long[][] blockKeys = new long[numUnits][2];
					for (int i = 0; i < numUnits; i++)
					{
						blockKeys[i][0] = rnd.nextLong();
						blockKeys[i][1] = rnd.nextLong();
					}

					byte[] original = new byte[numUnits * unitSize];
					rnd.nextBytes(original);

					byte[] input = original.clone();

					long t0 = System.currentTimeMillis();

					try (SecureBlockDevice device = SecureBlockDevice.create(new AccessCredentials("password".toCharArray(), ef, kgf, cmf, 100), blockDevice))
					{
						for (int i = 0; i < numUnits / blocksPerUnit; i++)
						{
							device.writeBlock(blocksPerUnit * i, input, blocksPerUnit * i * unitSize, blocksPerUnit * unitSize, blockKeys[i]);
						}
					}

					long t1 = System.currentTimeMillis();

					assertEquals(input, original);

					byte[] output = new byte[numUnits * unitSize];

					try (SecureBlockDevice device = SecureBlockDevice.open(blockDevice, new AccessCredentials("password".toCharArray()).setIterationCount(100)))
					{
						for (int i = 0; i < numUnits / blocksPerUnit; i++)
						{
							device.readBlock(blocksPerUnit * i, output, blocksPerUnit * i * unitSize, blocksPerUnit * unitSize, blockKeys[i]);
						}
					}

					long t2 = System.currentTimeMillis();

					assertEquals(output, input);

	//				System.out.printf("%4d %4d %s %s%n", t1-t0, t2-t1, kgf, ef);
				}
			}
		}
	}
}
