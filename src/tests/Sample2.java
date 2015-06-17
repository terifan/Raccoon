package tests;

import java.io.File;
import java.util.Arrays;
import org.terifan.raccoon.Database;
import org.terifan.raccoon.OpenOption;
import org.terifan.raccoon.io.ManagedBlockDevice;
import org.terifan.raccoon.io.MemoryBlockDevice;
import org.terifan.raccoon.util.Log;
import static tests.__TestUtils.*;


public class Sample2
{
	public static void main(String... args)
	{
		try
		{
			MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);
			ManagedBlockDevice managedBlockDevice = new ManagedBlockDevice(blockDevice);

			_KeyValue1K in = new _KeyValue1K("apple", createBuffer(0, 1000_000));
			_KeyValue1K out = new _KeyValue1K("apple");

			try (Database db = Database.open(managedBlockDevice, OpenOption.CREATE_NEW))
			{
				db.save(in);
				db.commit();
			}

			try (Database db = Database.open(managedBlockDevice, OpenOption.OPEN))
			{
				db.get(out);

				db.remove(out);
				db.commit();
			}
			
			Log.out.println(managedBlockDevice);
			
			Log.out.println(Arrays.equals(in.content, out.content));
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
	}
}
