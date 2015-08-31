package tests;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.OutputStream;
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
//			Log.LEVEL = 10;
			
			MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);
			ManagedBlockDevice managedBlockDevice = new ManagedBlockDevice(blockDevice);

			_BlobKey1K in = new _BlobKey1K("apple");

			try (Database db = Database.open(managedBlockDevice, OpenOption.CREATE_NEW))
			{
				try (OutputStream out = db.saveBlob(in))
				{
					out.write(new byte[100000]);
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
