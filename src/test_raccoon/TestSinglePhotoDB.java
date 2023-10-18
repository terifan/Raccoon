package test_raccoon;

import java.io.File;
import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import org.terifan.raccoon.RaccoonDatabase;
import org.terifan.raccoon.DatabaseOpenOption;
import org.terifan.raccoon.blockdevice.LobByteChannel;
import org.terifan.raccoon.blockdevice.LobOpenOption;
import org.terifan.raccoon.blockdevice.managed.SyncMode;
import org.terifan.raccoon.blockdevice.physical.FileBlockDevice;


public class TestSinglePhotoDB
{
	public static void main(String... args)
	{
		try
		{
			for (File file : new File("d:\\dev\\rdb_pictures\\in\\").listFiles())
			{
				byte[] imageData;
				try (FileInputStream in = new FileInputStream(file))
				{
					imageData = in.readAllBytes();
				}

				String name = file.getName();
				Path dst = Paths.get("d:\\dev\\rdb_pictures\\out\\" + name.substring(0, name.lastIndexOf('.')) + ".pic");
				Files.deleteIfExists(dst);

				try (FileBlockDevice f = new FileBlockDevice(dst, 512).setSyncMode(SyncMode.OFF); RaccoonDatabase db = new RaccoonDatabase(f, DatabaseOpenOption.CREATE, null))
				{
					try (LobByteChannel lob = db.getCollection("data").openLob("picture", LobOpenOption.CREATE))
					{
						lob.getMetadata().put("width", 3200).put("height", 2400).putEpochTime("modified", file.lastModified()).put("size", file.length()).put("name", name);
						lob.writeAllBytes(imageData);
					}
					db.commit();
				}

				try (FileBlockDevice fst = new FileBlockDevice(dst, 512); RaccoonDatabase db = new RaccoonDatabase(fst, DatabaseOpenOption.OPEN, null))
				{
					db.getCollection("data").listAll().forEach(System.out::println);

					db.getCollection("data").listAll().forEach(e ->
					{
						System.out.println(e.getDocument("$meta"));
						try
						{
							try (LobByteChannel lob = db.getCollection("data").openLob(e, LobOpenOption.READ))
							{
								System.out.println(lob.getMetadata());
								if (!Arrays.equals(imageData, lob.readAllBytes()))
								{
									throw new IllegalStateException();
								}
							}
						}
						catch (Exception ex)
						{
							ex.printStackTrace(System.out);
						}
					});
				}
			}
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
	}
}
