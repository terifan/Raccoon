package org.terifan.raccoon.io;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import org.terifan.raccoon.CompressionParam;
import static org.terifan.raccoon.CompressionParam.ZLE;
import org.terifan.raccoon.util.Log;
import static org.testng.Assert.*;
import static org.testng.AssertJUnit.assertEquals;
import org.testng.annotations.Test;


public class ZLENGTest
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

		Random rnd = new Random(7);
		for (int i = 0; i < length; i++)
		{
			if (rnd.nextBoolean())
			{
				input[srcOffset + i] = (byte)rnd.nextInt();
			}
			else
			{
				input[srcOffset + i] = (byte)0;
			}
		}

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		new ZeroCompressor(0).compress(input, srcOffset, length, baos);

		new ZeroCompressor(0).decompress(baos.toByteArray(), 0, baos.size(), output, dstOffset, length);

		assertEquals(Arrays.copyOfRange(input, srcOffset, srcOffset + length), Arrays.copyOfRange(output, dstOffset, dstOffset + length));
	}
}
