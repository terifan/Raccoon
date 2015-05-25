package sample;

import java.util.Random;
import org.terifan.raccoon.Key;
import org.terifan.raccoon.io.ManagedBlockDevice;
import org.terifan.raccoon.io.MemoryBlockDevice;
import org.terifan.raccoon.util.Log;


public class Sample1
{
	public static void main(String... args)
	{
		try
		{
			Log.LEVEL = 4;
			
			MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);

			try (ManagedBlockDevice managedBlockDevice = new ManagedBlockDevice(blockDevice))
			{
				byte[] buf = new byte[512];
				new Random().nextBytes(buf);
				long i = managedBlockDevice.allocBlock(1);
				managedBlockDevice.writeBlock(i, buf, 0, 512, 64);
				managedBlockDevice.commit();
			}

			try (ManagedBlockDevice managedBlockDevice = new ManagedBlockDevice(blockDevice))
			{
				byte[] buf = new byte[512];
				managedBlockDevice.readBlock(0, buf, 0, 512, 64);
				Log.hexDump(buf);
			}

			for (byte[] buf : blockDevice.getStorage().values())
			{
				Log.hexDump(buf);
				Log.out.println();
			}

//			try (Database db = Database.open(new File("d:/sample.db"), OpenOption.CREATE_NEW))
//			{
//				db.save(new Item("test1", new String(new byte[7000])));
//				db.save(new Item("test2", new String(new byte[7000])));
//				db.save(new Item("test3", new String(new byte[7000])));
//				db.save(new Item("test4", new String(new byte[7000])));
//				db.commit();
//			}
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
	}


	static class Item
	{
		@Key String name;
		String value;


		public Item()
		{
		}


		public Item(String aName, String aValue)
		{
			name = aName;
			value = aValue;
		}
	}
}
