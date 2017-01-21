package tests;

import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import org.terifan.raccoon.Database;
import org.terifan.raccoon.Key;
import org.terifan.raccoon.OpenOption;
import org.terifan.raccoon.io.MemoryBlockDevice;
import org.terifan.raccoon.util.Log;


public class ConcurrencyTest
{
	private static Database db;
	private static Random rnd = new Random();
	private static String[] names = new String[1000];
	private static byte[][] datas = new byte[names.length][];

	public static void main(String... args)
	{
		try
		{
			HashSet<String> unique = new HashSet<>();
			for (int i = 0; i < names.length; i++)
			{
				do
				{
					names[i] = "";
					for (int j = 6 + rnd.nextInt(10); --j >= 0;)
					{
						names[i] += (char)(97 + rnd.nextInt(26));
					}
				} while (!unique.add(names[i]));

				datas[i] = new byte[8192 + rnd.nextInt(8192)];
				rnd.nextBytes(datas[i]);
			}

			new File("e:/test.dat").delete();

			MemoryBlockDevice blockDevice = new MemoryBlockDevice(4096);
//			FileBlockDevice blockDevice = new FileBlockDevice(new File("e:/test.dat"), 4096, false);

			for (int k = 0; k < 5; k++)
			{
				try (Database tmp = Database.open(blockDevice, OpenOption.CREATE))
				{
					db = tmp;

					try (__FixedThreadExecutor executor = new __FixedThreadExecutor(50))
					{
						for (int i = 0; i < names.length; i++)
						{
							executor.submit(new Reader(rnd.nextInt(names.length)));
							executor.submit(new Reader(rnd.nextInt(names.length)));
							executor.submit(new Reader(rnd.nextInt(names.length)));
							executor.submit(new Reader(rnd.nextInt(names.length)));
							executor.submit(new Reader(rnd.nextInt(names.length)));
							executor.submit(new Reader(rnd.nextInt(names.length)));
							executor.submit(new Reader(rnd.nextInt(names.length)));
							executor.submit(new Creator(i));
						}
					}

					db.commit();
				}

				System.out.println();
			}
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
	}


	private static class Creator implements Runnable
	{
		private int mIndex;


		public Creator(int aIndex)
		{
			mIndex = aIndex;
		}


		@Override
		public void run()
		{
			try
			{
				db.save(new Entry(names[mIndex], datas[mIndex], mIndex));
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


	private static class Reader implements Runnable
	{
		private int mIndex;


		public Reader(int aIndex)
		{
			mIndex = aIndex;
		}


		@Override
		public void run()
		{
			try
			{
				Entry entry = new Entry(names[mIndex]);
				if (db.tryGet(entry))
				{
					if (entry.mIndex != mIndex)
					{
//						System.out.print("("+entry.mIndex+"/"+mIndex+")");
						System.out.print("*");
					}
					else if (!Arrays.equals(entry.mBuffer, datas[mIndex]))
					{
//						System.out.print("["+entry.mIndex+"/"+mIndex+"]");
						System.out.print("#");
					}
					else
					{
						System.out.print("+");
					}
				}
				else
				{
					System.out.print(".");
				}
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


	public static class Entry
	{
		public int mIndex;
		@Key public String mName;
		public byte[] mBuffer;


		public Entry(String aName)
		{
			mName = aName;
		}


		public Entry(String aName, byte[] aBuffer, int aIndex)
		{
			mName = aName;
			mBuffer = aBuffer;
			mIndex = aIndex;
		}
	}
}
