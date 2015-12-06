package org.terifan.raccoon.io;

import java.util.Arrays;
import org.terifan.raccoon.util.Log;

/**
 * Zero-run-length encoding. This is a fast and simple algorithm to eliminate
 * runs of zeroes.
 */
public class ZLE implements Compressor
{
	private final static int LIMIT = 240;
	
	
	ZLE()
	{
	}
	

	@Override
	public int compress(byte[] aInput, int aInputOffset, int aInputLength, byte[] aOutput, int aOutputOffset, int aOutputLimit)
	{
		int src = aInputOffset;
		int dst = aOutputOffset;
		int dstEnd = aOutputOffset + aOutputLimit;
		
		for (; src < aInputLength && dst < dstEnd - 1;)
		{
			int first = src;
			int len = dst++;
			if (aInput[src] == 0)
			{
				int end = Math.min(aInputLength, src + 256 - LIMIT);
				while (src < end && aInput[src] == 0)
				{
					src++;
				}
				aOutput[len] = (byte) (src - first - 1 + LIMIT);
			}
			else
			{
				int last = src + LIMIT;
				int end = Math.min(aInputLength, last) - 1;
				while (dst < aInputLength - 1 && src < end && (aInput[src] != 0 || aInput[src + 1] != 0))
				{
					aOutput[dst++] = aInput[src++];
				}
				if (aInput[src] != 0)
				{
					aOutput[dst++] = aInput[src++];
				}
				aOutput[len] = (byte) (src - first - 1);
			}
		}

		return src == aInputOffset + aInputLength ? dst : Compressor.COMPRESSION_FAILED;
	}


	@Override
	public void decompress(byte[] aInput, int aInputOffset, int aInputLength, byte[] aOutput, int aOutputOffset, int aOutputLength)
	{
		int src = aInputOffset;
		int dst = aOutputOffset;

		while (src < aInputLength && dst < aOutputLength)
		{
			int len = 1 + (255 & aInput[src++]);
			if (len <= LIMIT)
			{
				while (len-- != 0)
				{
					aOutput[dst++] = aInput[src++];
				}
			}
			else
			{
				len -= LIMIT;
				while (len-- != 0)
				{
					aOutput[dst++] = 0;
				}
			}
		}

		if (dst != aOutputOffset + aOutputLength)
		{
			throw new IllegalStateException("Failed to decompress data.");
		}
	}
	
	
	public static void main(String... args)
	{
		try
		{
			byte[] input = new byte[32768];
			byte[] output = new byte[32768];

			Arrays.fill(input, 0, 100, (byte)'a');
			Arrays.fill(input, input.length-20, input.length, (byte)'a');

			Log.out.println(new ZLE().compress(input, 0, input.length, output, 0, output.length));
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
	}
}
