package org.terifan.raccoon.storage;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import org.terifan.raccoon.util.ByteBlockOutputStream;
import static org.testng.AssertJUnit.assertEquals;
import org.testng.annotations.Test;


public class ZeroCompressorNGTest
{
	@Test
	public void testCompression() throws IOException
	{
		int length = 128 * 1024;
		int srcOffset = 4;
		int dstOffset = 2;

		byte[] input = new byte[srcOffset + length + 100];
		byte[] output = new byte[input.length];

		Arrays.fill(input, (byte)'x');
		Arrays.fill(output, (byte)'z');

		Random rnd = new Random(1);

		for (int i = 0; i < length; i++)
		{
			if (rnd.nextInt(100) < 20)
			{
				input[srcOffset + i] = (byte)rnd.nextInt();
			}
			else
			{
				input[srcOffset + i] = (byte)0;
			}
		}

		ByteBlockOutputStream baos = new ByteBlockOutputStream(512);
		new ZLE(0).compress(input, srcOffset, length, baos);

		new ZLE(0).decompress(baos.getBuffer(), 0, baos.size(), output, dstOffset, length);

		assertEquals(Arrays.copyOfRange(input, srcOffset, srcOffset + length), Arrays.copyOfRange(output, dstOffset, dstOffset + length));
	}


	@Test
	public void testCompress()
	{
	}


	@Test
	public void testDecompress() throws Exception
	{
	}
}
