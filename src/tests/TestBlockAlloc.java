package tests;

import org.terifan.raccoon.io.ManagedBlockDevice;
import org.terifan.raccoon.io.MemoryBlockDevice;
import org.terifan.raccoon.util.Log;

public class TestBlockAlloc
{
	public static void main(String... args)
	{
		try
		{
			Log.LEVEL = 10;

			ManagedBlockDevice blockDevice = new ManagedBlockDevice(new MemoryBlockDevice(1024));
			
			long offset = blockDevice.allocBlock(4);
			System.out.println(offset);
			
			blockDevice.freeBlock(offset, 4);

			offset = blockDevice.allocBlock(4);
			System.out.println(offset);
			
			blockDevice.freeBlock(offset, 4);

			offset = blockDevice.allocBlock(4);
			System.out.println(offset);
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
	}
}
