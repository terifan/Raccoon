package sample;

import java.util.Arrays;
import java.util.Random;
import org.terifan.raccoon.Key;
import org.terifan.raccoon.io.AccessCredentials;
import org.terifan.raccoon.io.IManagedBlockDevice;
import org.terifan.raccoon.io.ManagedBlockDevice;
import org.terifan.raccoon.io.MemoryBlockDevice;
import org.terifan.raccoon.io.SecureBlockDevice;
import org.terifan.raccoon.util.Log;


public class Sample1
{
	public static void main(String... args)
	{
		try
		{
			Log.LEVEL = 4;

			MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);

			byte[] in = new byte[512];
			byte[] inExtra = new byte[384];
			new Random().nextBytes(in);
			new Random().nextBytes(inExtra);

			AccessCredentials accessCredentials = new AccessCredentials("password");

			try (IManagedBlockDevice managedBlockDevice = new ManagedBlockDevice(new SecureBlockDevice(blockDevice, accessCredentials)))
			{
				long i = managedBlockDevice.allocBlock(1);
				managedBlockDevice.setExtraData(inExtra);
				managedBlockDevice.writeBlock(i, in, 0, 512, 64);
				managedBlockDevice.commit();
			}

			Log.out.println("-----------------------------------------------------------------------------------------------------------------------------------------------------------------------");
			byte[] out = new byte[512];
			byte[] outExtra;

			try (IManagedBlockDevice managedBlockDevice = new ManagedBlockDevice(new SecureBlockDevice(blockDevice, accessCredentials)))
			{
				outExtra = managedBlockDevice.getExtraData();
				managedBlockDevice.readBlock(0, out, 0, 512, 64);
			}

			Log.out.println(Arrays.equals(in, out));
			Log.out.println(Arrays.equals(inExtra, outExtra));

			for (byte[] buf : blockDevice.getStorage().values())
			{
				Log.hexDump(buf);
				Log.out.println();
			}
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
