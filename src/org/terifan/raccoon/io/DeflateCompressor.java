package org.terifan.raccoon.io;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;
import org.terifan.raccoon.util.Log;


public class DeflateCompressor implements Compressor
{
	private int mLevel;


	public DeflateCompressor(int aLevel)
	{
		mLevel = aLevel;
	}


	@Override
	public boolean compress(byte[] aInput, int aInputOffset, int aInputLength, ByteArrayOutputStream aOutputStream)
	{
		try
		{
			try (DeflaterOutputStream dis = new DeflaterOutputStream(aOutputStream, new Deflater(mLevel)))
			{
				dis.write(aInput, aInputOffset, aInputLength);
			}

			return aOutputStream.size() < aInputLength;
		}
		catch (IOException e)
		{
			e.printStackTrace(Log.out);
		}

		return false;
	}


	@Override
	public void decompress(byte[] aInput, int aInputOffset, int aInputLength, byte[] aOutput, int aOutputOffset, int aOutputLength) throws IOException
	{
		try (InflaterInputStream iis = new InflaterInputStream(new ByteArrayInputStream(aInput, aInputOffset, aInputLength)))
		{
			for (int position = aOutputOffset;;)
			{
				int len = iis.read(aOutput, position, aOutputLength - position);

				if (len <= 0)
				{
					break;
				}

				position += len;
			}
		}
	}
}
