package tests;

import java.util.ArrayList;
import java.util.Random;
import org.terifan.raccoon.Database;
import org.terifan.raccoon.OpenOption;
import org.terifan.raccoon.io.MemoryBlockDevice;
import org.terifan.raccoon.util.Log;


public class ConcurrencyTest
{
	private static Database db;
	private static String[] keys;


	public static void main(String... args)
	{
		try
		{
			Random rnd = new Random();

			keys = new String[10000];
			char[] buf = new char[40];
			for (int i = 0; i < keys.length; i++)
			{
				for (int j = 0; j < 40; j++)
				{
					buf[j] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-/".charAt(rnd.nextInt(64));
				}
				keys[i] = new String(buf, 0, 5 + rnd.nextInt(35));
			}

			MemoryBlockDevice blockDevice = new MemoryBlockDevice(4096);

			db = Database.open(blockDevice, OpenOption.CREATE_NEW);

			ArrayList<Reader> readers = new ArrayList<>();
			for (int i = 0; i < 6; i++)
			{
				Reader reader = new Reader();
				readers.add(reader);
				reader.start();
			}

			try (__FixedThreadExecutor executor = new __FixedThreadExecutor(2))
			{
				for (int k = 0; k < 10; k++)
				{
					for (int i = 0; i < keys.length; i++)
					{
						final int j = i;
						executor.submit(()->
						{
							try
							{
								db.save(new _Fruit1K(keys[j], 52.12));
							}
							catch (Exception e)
							{
								synchronized (ConcurrencyTest.class)
								{
									e.printStackTrace(Log.out);
								}
							}
						});
					}
				}
			}

			for (String key : keys)
			{
				if (!db.get(new _Fruit1K(key)))
				{
					synchronized (ConcurrencyTest.class)
					{
						Log.out.println("err");
					}
				}
			}

			Log.out.println("#");

			for (Reader reader : readers)
			{
				reader.stop = true;
				Log.out.println(reader.count);
			}
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
	}


	private static class Reader extends Thread
	{
		boolean stop;
		int count;

		@Override
		public void run()
		{
			Random rnd = new Random();

			while (!stop)
			{
				try
				{
					db.get(new _Fruit1K(keys[rnd.nextInt(keys.length)]));
					count++;
				}
				catch (Exception e)
				{
					synchronized (ConcurrencyTest.class)
					{
						e.printStackTrace(Log.out);
					}
				}
			}
		}
	}
}