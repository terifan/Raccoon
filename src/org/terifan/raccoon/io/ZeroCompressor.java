package org.terifan.raccoon.io;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;

/**
 * Zero-run-length encoding. This is a fast and simple algorithm to eliminate
 * runs of zeroes.
 */
public class ZeroCompressor implements Compressor
{
	private int mPageSize;


	ZeroCompressor(int aPageSize)
	{
		mPageSize = aPageSize;
	}


	@Override
	public boolean compress(byte[] aInput, int aInputOffset, int aInputLength, ByteArrayOutputStream aOutputStream)
	{
		try
		{
			for (int i = 0; i < aInputLength;)
			{
				int j = i;

				if (aInput[aInputOffset + i] == 0)
				{
					for (; j < aInputLength && aInput[aInputOffset + j] == 0; j++)
					{
					}
					ByteArrayBuffer.writeVar32(aOutputStream, 2 * (j-i-1) + 1);
				}
				else
				{
					for (; j == aInputLength - 1 || j < aInputLength && !(aInput[aInputOffset + j] == 0 && aInput[aInputOffset + j + 1] == 0); j++)
					{
					}
					ByteArrayBuffer.writeVar32(aOutputStream, 2 * (j-i-1));
					aOutputStream.write(aInput, aInputOffset + i, j-i);
				}

				i = j;
			}
		}
		catch (Exception e)
		{
			e.printStackTrace(Log.out);
		}

		return true;
	}


	@Override
	public void decompress(byte[] aInput, int aInputOffset, int aInputLength, byte[] aOutput, int aOutputOffset, int aOutputLength) throws IOException
	{
		ByteArrayBuffer buffer = new ByteArrayBuffer(aInput).position(aInputOffset);

		for (int position = 0; buffer.position() < aInputOffset + aInputLength;)
		{
			int code = buffer.readVar32();
			int len = (code >> 1) + 1;

			if (position + len > aOutputLength)
			{
				throw new IOException();
			}

			if ((code & 1) == 1)
			{
				Arrays.fill(aOutput, aOutputOffset + position, aOutputOffset + position + len, (byte)0);
			}
			else
			{
				buffer.read(aOutput, aOutputOffset + position, len);
			}

			position += len;
		}
	}
}
