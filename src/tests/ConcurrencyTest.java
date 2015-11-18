package tests;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import org.terifan.raccoon.Database;
import org.terifan.raccoon.OpenOption;
import org.terifan.raccoon.io.AccessCredentials;
import org.terifan.raccoon.io.MemoryBlockDevice;
import org.terifan.raccoon.util.Log;

// 2:54

public class ConcurrencyTest 
{
	public static void main(String... args)
	{
		try
		{
			Random rnd = new Random(1);
			String[] keys = new String[100000];
			char[] buf = new char[40];
			for (int i = 0; i < keys.length; i++)
			{
				for (int j = 0; j < 40; j++)
				{
					buf[j] = "abcdefghijklmnopqrstuvwxyz".charAt(rnd.nextInt(26));
				}
				keys[i] = new String(buf, 0, 5 + rnd.nextInt(35));
			}

			AccessCredentials accessCredentials = new AccessCredentials("password");

			MemoryBlockDevice blockDevice = new MemoryBlockDevice(4096);

			try (Database db = Database.open(blockDevice, OpenOption.CREATE_NEW, accessCredentials))
			{
				try (__FixedThreadExecutor executor2 = new __FixedThreadExecutor(4))
				{
					try (__FixedThreadExecutor executor = new __FixedThreadExecutor(4))
					{
						final AtomicInteger in = new AtomicInteger();

						for (int i = 0; i < keys.length; i++)
						{
							executor.submit(()->
							{
								try
								{
									final String key = keys[in.getAndIncrement()];
									db.save(new _Fruit1K(key, 52.12));

									executor2.submit(()->
									{
										try
										{
											if (!db.get(new _Fruit1K(key)))
											{
												synchronized (ConcurrencyTest.class)
												{
													Log.out.println("err");
												}
											}
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

//				db.commit();
//
//				for (;;){
//					Log.out.println("#");
//				try (__FixedThreadExecutor executor = new __FixedThreadExecutor(8))
//				{
//					final AtomicInteger in = new AtomicInteger();
//
//					for (int i = 0; i < 10000; i++)
//					{
//						executor.submit(()->
//						{
//							try
//							{
//								final String key = "apple" + in.incrementAndGet();
//								if (!db.get(new _Fruit1K(key)))
//								{
//									synchronized (ConcurrencyTest.class)
//									{
//										Log.out.println("err");
//									}
//								}
//							}
//							catch (Exception e)
//							{
//								synchronized (ConcurrencyTest.class)
//								{
//									e.printStackTrace(Log.out);
//								}
//							}
//						});
//					}
//				}}
			}
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
	}
}
