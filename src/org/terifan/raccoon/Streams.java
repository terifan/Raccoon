package org.terifan.raccoon;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;


final class Streams
{
	private Streams()
	{
	}


	public static byte[] readAll(InputStream aInputStream) throws IOException
	{
		try
		{
			byte [] buffer = new byte[4096];
			ByteArrayOutputStream baos = new ByteArrayOutputStream();

			for (;;)
			{
				int len = aInputStream.read(buffer);

				if (len <= 0)
				{
					break;
				}

				baos.write(buffer, 0, len);
			}

			return baos.toByteArray();
		}
		finally
		{
			aInputStream.close();
		}
	}


	public static int transfer(InputStream aInputStream, OutputStream aOutputStream) throws IOException
	{
		byte[] buffer = new byte[4096];
		int total = 0;

		for (;;)
		{
			int len = aInputStream.read(buffer);

			if (len <= 0)
			{
				break;
			}

			aOutputStream.write(buffer, 0, len);
			total += len;
		}

		return total;
	}
}