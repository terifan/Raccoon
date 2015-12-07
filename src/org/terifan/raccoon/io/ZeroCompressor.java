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
	ZeroCompressor(int aPageSize)
	{
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
					ByteArrayBuffer.writeVar32(aOutputStream, j - i); // 1..n
				}
				else
				{
					for (; j == aInputLength - 1 || j < aInputLength - 2 && (aInput[aInputOffset + j] != 0 || aInput[aInputOffset + j + 1] != 0); j++)
					{
					}

					ByteArrayBuffer.writeVar32(aOutputStream, i - j + 1); // -n..0
					aOutputStream.write(aInput, aInputOffset + i, j - i);
				}

				i = j;
			}

			return true;
		}
		catch (Exception e)
		{
			e.printStackTrace(Log.out);

			return false;
		}
	}


	@Override
	public void decompress(byte[] aInput, int aInputOffset, int aInputLength, byte[] aOutput, int aOutputOffset, int aOutputLength) throws IOException
	{
		ByteArrayBuffer buffer = new ByteArrayBuffer(aInput).position(aInputOffset);

		for (int position = 0; buffer.position() < aInputOffset + aInputLength;)
		{
			int len = buffer.readVar32();

			if (len > 0)
			{
				Arrays.fill(aOutput, aOutputOffset + position, aOutputOffset + position + len, (byte)0);
			}
			else
			{
				len = - (len - 1);
				buffer.read(aOutput, aOutputOffset + position, len);
			}

			position += len;
		}
	}
}
