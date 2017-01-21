package tests;

import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import org.terifan.raccoon.Database;
import org.terifan.raccoon.Key;
import org.terifan.raccoon.OpenOption;
import org.terifan.raccoon.io.IPhysicalBlockDevice;
import org.terifan.raccoon.io.MemoryBlockDevice;
import org.terifan.raccoon.util.Log;


public class ConcurrencyTest
{
	private static Database db;
	private static Random rnd = new Random();
	private static String[] names = new String[1000];
	private static byte[][] buffers = new byte[names.length][];


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

				buffers[i] = new byte[8192 + rnd.nextInt(8192)];
				rnd.nextBytes(buffers[i]);
			}

			File file = new File("e:/test.dat");

			for (int n = 0; n < 5; n++)
			{
				MemoryBlockDevice _blockDevice = new MemoryBlockDevice(1024);
//				FileBlockDevice _blockDevice = new FileBlockDevice(file, 4096, false);
//				file.delete();

				try (IPhysicalBlockDevice blockDevice = _blockDevice)
				{
					for (int k = 0; k < 2; k++)
					{
						try (Database tmp = Database.open(blockDevice, OpenOption.CREATE))
						{
							db = tmp;

							try (__FixedThreadExecutor executor = new __FixedThreadExecutor(8))
							{
								for (int i = 0; i < names.length; i++)
								{
									executor.submit(new GetEntry(rnd.nextInt(names.length)));
									executor.submit(new GetEntry(rnd.nextInt(names.length)));
									executor.submit(new GetEntry(rnd.nextInt(names.length)));
									executor.submit(new PutEntry(i));
								}
							}

							db.commit();
						}

						System.out.println();
					}
				}

				System.out.println();
			}
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
	}


	private static class PutEntry implements Runnable
	{
		private int mIndex;


		public PutEntry(int aIndex)
		{
			mIndex = aIndex;
		}


		@Override
		public void run()
		{
			try
			{
				db.save(new Entry(names[mIndex], buffers[mIndex], mIndex));
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


	private static class GetEntry implements Runnable
	{
		private int mIndex;


		public GetEntry(int aIndex)
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
						System.out.print("*");
					}
					else if (!Arrays.equals(entry.mBuffer, buffers[mIndex]))
					{
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
