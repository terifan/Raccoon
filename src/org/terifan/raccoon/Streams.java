package org.terifan.raccoon;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;


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
}