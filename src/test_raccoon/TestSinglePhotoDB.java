package test_raccoon;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import org.terifan.raccoon.RaccoonDatabase;
import org.terifan.raccoon.DatabaseOpenOption;
import org.terifan.raccoon.blockdevice.LobByteChannel;
import org.terifan.raccoon.blockdevice.LobOpenOption;
import org.terifan.raccoon.blockdevice.managed.SyncMode;
import org.terifan.raccoon.blockdevice.storage.FileBlockStorage;
import org.terifan.raccoon.document.Document;
import org.terifan.raccoon.document.ObjectId;


public class TestSinglePhotoDB
{
	public static void main(String... args)
	{
		try
		{
			for (File file : new File("d:\\dev\\rdb_pictures\\in\\").listFiles())
			{
				String name = file.getName();
				Path dst = Paths.get("d:\\dev\\rdb_pictures\\out\\" + name.substring(0, name.lastIndexOf('.')) + ".pic");
				Files.deleteIfExists(dst);

				byte[] imageData = Files.readAllBytes(file.toPath());

				try (RaccoonDatabase db = new RaccoonDatabase(new FileBlockStorage(dst, 4096).setSyncMode(SyncMode.OFF), DatabaseOpenOption.CREATE, null))
				{
					try (LobByteChannel lob = db.getDirectory("pics").open(ObjectId.randomId(), LobOpenOption.CREATE, Document.of("leaf:1048576,compression:none")))
					{
						lob.getMetadata().put("width", 3200).put("height", 2400).putEpochTime("modified", file.lastModified()).put("name", name);
						lob.writeAllBytes(imageData);
//						lob.flush();
//						lob.scan();
					}
					db.commit();
				}

				try (FileBlockStorage fst = new FileBlockStorage(dst, 4096); RaccoonDatabase db = new RaccoonDatabase(fst, DatabaseOpenOption.OPEN, null))
				{
					db.getDirectory("pics").forEach((id, meta) ->
					{
						System.out.println(id + " = " + meta.toTypedJson());

						try
						{
							try (LobByteChannel lob = db.getDirectory("pics").open(id, LobOpenOption.READ))
							{
//								System.out.println(lob.getMetadata());

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
