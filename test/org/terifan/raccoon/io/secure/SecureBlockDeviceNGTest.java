package org.terifan.raccoon.io.secure;

import org.terifan.raccoon.io.physical.MemoryBlockDevice;
import java.io.IOException;
import java.util.Random;
import org.testng.annotations.Test;
import static org.testng.Assert.*;


public class SecureBlockDeviceNGTest
{
	@Test
	public void testSomeMethod() throws IOException
	{
		KeyGenerationFunction kgf = KeyGenerationFunction.SHA512;
//		for (KeyGenerationFunction kgf : KeyGenerationFunction.values())
		{
			EncryptionFunction ef = EncryptionFunction.AESTwofishSerpent;
//			for (EncryptionFunction ef : EncryptionFunction.values())
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

				try (SecureBlockDevice device = new SecureBlockDevice(blockDevice, new AccessCredentials("password", ef, kgf)))
				{
					for (int i = 0; i < numUnits / blocksPerUnit; i++)
					{
						device.writeBlock(blocksPerUnit * i, input, blocksPerUnit * i * unitSize, blocksPerUnit * unitSize, blockKeys[i]);
					}
				}

				assertEquals(input, original);

				byte[] output = new byte[numUnits * unitSize];

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
	}
}
