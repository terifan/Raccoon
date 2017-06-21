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

		try (SecureBlockDevice device = SecureBlockDevice.create(blockDevice, accessCredentials))
		{
			device.writeBlock(0, new byte[4096], 0, 4096, 0);
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
				Random rnd = new Random();

				int unitSize = 512;
				int numUnits = 32;
				int blocksPerUnit = 4;

				MemoryBlockDevice blockDevice = new MemoryBlockDevice(unitSize);

				long[] blockKeys = new long[numUnits];
				for (int i = 0; i < numUnits; i++)
				{
					blockKeys[i] = rnd.nextLong();
				}

				byte[] original = new byte[numUnits * unitSize];
				rnd.nextBytes(original);

				byte[] input = original.clone();

				try (SecureBlockDevice device = SecureBlockDevice.create(blockDevice, new AccessCredentials("password".toCharArray(), ef, kgf, 100)))
				{
					for (int i = 0; i < numUnits / blocksPerUnit; i++)
					{
						device.writeBlock(blocksPerUnit * i, input, blocksPerUnit * i * unitSize, blocksPerUnit * unitSize, blockKeys[i]);
					}
				}

				assertEquals(input, original);

				byte[] output = new byte[numUnits * unitSize];

				try (SecureBlockDevice device = SecureBlockDevice.open(blockDevice, new AccessCredentials("password".toCharArray()).setIterationCount(100)))
				{
					for (int i = 0; i < numUnits / blocksPerUnit; i++)
					{
						device.readBlock(blocksPerUnit * i, output, blocksPerUnit * i * unitSize, blocksPerUnit * unitSize, blockKeys[i]);
					}
				}

				assertEquals(output, input);
			}
		}
	}
}
