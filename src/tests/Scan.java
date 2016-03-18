package tests;

import java.io.ByteArrayInputStream;
import java.io.File;
import org.terifan.raccoon.Database;
import org.terifan.raccoon.OpenOption;
import org.terifan.raccoon.TableMetadata;
import org.terifan.raccoon.io.AccessCredentials;
import org.terifan.raccoon.util.Log;


public class Scan
{
	public static void main(String... args)
	{
		try
		{
			Log.LEVEL = 0;

			AccessCredentials accessCredentials = new AccessCredentials("password");

			try (Database db = Database.open(new File("d:/sample.db"), OpenOption.CREATE_NEW, accessCredentials))
			{
				for (int i = 0; i < 1000; i++)
				{
					db.save(new _Fruit1K("apple"+i, 52.12));
					db.save(new _Fruit1K("orange"+i, 47.78));
					db.save(new _Fruit1K("banana"+i, 89.45));
				}
				db.save(new _KeyValue1K("inline", new byte[1000_000]));
				db.save(new _BlobKey1K("blob"), new ByteArrayInputStream(new byte[5*1024*1024]));
				db.commit();
			}

			try (Database db = Database.open(new File("d:/sample.db"), OpenOption.OPEN, accessCredentials))
			{
				db.scan();
			}
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
	}
}