package org.terifan.security.messagedigest;

import java.security.MessageDigest;
import java.util.Random;


public class Test
{
	public static void main(String ... args)
	{
		try
		{
			MessageDigest[] digests =
			{
				new SHA3(512),
				new Skein512(),
				new Whirlpool(),
				new SHA512()
			};

			byte[] in = new byte[16 * 1024];
			new Random().nextBytes(in);

			for (MessageDigest digest : digests)
			{
				System.out.printf("%20s", digest.getAlgorithm());
			}
			System.out.println();
			for (int j = 0; j < 100; j++)
			{
				for (MessageDigest digest : digests)
				{
					long t = System.currentTimeMillis();
					for (int i = 0; i < 1000; i++)
					{
						digest.digest(in);
					}
					t = System.currentTimeMillis() - t;

					System.out.printf("%20d", t);
				}
				System.out.println();
			}
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
	}
}
