package tests;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.terifan.raccoon.Database;
import org.terifan.raccoon.OpenOption;
import org.terifan.raccoon.io.AccessCredentials;
import org.terifan.raccoon.io.MemoryBlockDevice;
import org.terifan.raccoon.util.Log;

public class ConcurrencyTest 
{
	public static void main(String... args)
	{
		try
		{
			AccessCredentials accessCredentials = new AccessCredentials("password");

			MemoryBlockDevice blockDevice = new MemoryBlockDevice(4096);

			try (Database db = Database.open(blockDevice, OpenOption.CREATE_NEW, accessCredentials))
			{
				try (__FixedThreadExecutor executor = new __FixedThreadExecutor(1f))
				{
					final AtomicInteger in = new AtomicInteger();

					for (int i = 0; i < 10000; i++)
					{
						executor.submit(()->
						{
							try
							{
								final String key = "apple" + in.incrementAndGet();
								db.save(new _Fruit1K(key, 52.12));

								executor.submit(()->
								{
									try
									{
										if (!db.get(new _Fruit1K(key)))
										{
											Log.out.println("err");
										}
									}
									catch (Exception e)
									{
										e.printStackTrace(Log.out);
									}
								});
							}
							catch (Exception e)
							{
								e.printStackTrace(Log.out);
							}
						});
					}
					
					Thread.sleep(10000);
				}
				db.commit();
			}
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
	}
}
