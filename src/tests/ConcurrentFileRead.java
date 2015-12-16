package tests;

import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.Future;
import org.terifan.raccoon.util.Log;


public class ConcurrentFileRead
{
	public static void main(String ... args)
	{
		try
		{
			int n = 10000;
			byte[][] in = new byte[n][4096];
			int[] order = new int[n];
			for (int i = 0; i < n; i++)
			{
				order[i] = i;
			}
			for (int i = 0; i < n; i++)
			{
				int x = new Random().nextInt(n);
				int j = order[i];
				order[i] = order[x];
				order[x] = j;
			}

			try (FileChannel sbc = FileChannel.open(Paths.get("d:/sample.db"), StandardOpenOption.READ, StandardOpenOption.WRITE))
			{
				try (__FixedThreadExecutor executor = new __FixedThreadExecutor(1f))
				{
					for (int i = 0; i < n; i++)
					{
						final int j = order[i];
						executor.submit(()->
						{
							try
							{
								ByteBuffer buf = ByteBuffer.wrap(in[j]);
								sbc.read(buf, 4096*j);
							}
							catch (Exception e)
							{
								e.printStackTrace(Log.out);
							}
						});
					}
				}
			}

			try (FileInputStream fis = new FileInputStream("d:/sample.db"))
			{
				byte[] match = new byte[4096];
				for (int i = 0; i < n; i++)
				{
					fis.read(match);

					if (!Arrays.equals(match, in[i]))
					{
						Log.out.println("err " + i);
					}
				}
			}
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
	}
}
