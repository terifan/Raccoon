package tests;

import java.io.File;
import java.util.ArrayList;
import java.util.Random;
import org.terifan.raccoon.Database;
import org.terifan.raccoon.OpenOption;
import org.terifan.raccoon.io.FileBlockDevice;
import org.terifan.raccoon.util.Log;

// 1:10

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

//			MemoryBlockDevice blockDevice = new MemoryBlockDevice(4096);
			FileBlockDevice blockDevice = new FileBlockDevice(new File("d:/test.dat"), 4096, false);

			db = Database.open(blockDevice, OpenOption.CREATE_NEW);

			ArrayList<Reader> readers = new ArrayList<>();
			for (int i = 0; i < 6; i++)
			{
				Reader reader = new Reader();
				readers.add(reader);
				reader.start();
			}

			try (__FixedThreadExecutor executor = new __FixedThreadExecutor(8))
			{
				for (int k = 0; k < 10; k++)
				{
					for (String key : keys)
					{
						executor.submit(()->
						{
							try
							{
								db.save(new _Fruit1K(key, 52.12));
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
				if (!db.tryGet(new _Fruit1K(key)))
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
					db.tryGet(new _Fruit1K(keys[rnd.nextInt(keys.length)]));
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
