package test_raccoon;

import java.io.File;
import java.nio.file.Files;
import org.terifan.raccoon.RaccoonDatabase;
import org.terifan.raccoon.DatabaseOpenOption;
import org.terifan.raccoon.RaccoonBuilder;
import org.terifan.raccoon.RaccoonDirectory;
import org.terifan.raccoon.blockdevice.LobByteChannel;
import org.terifan.raccoon.blockdevice.LobOpenOption;
import org.terifan.raccoon.document.ObjectId;


public class TestLOB
{
	public static void main(String... args)
	{
		try
		{
			RaccoonBuilder builder = new RaccoonBuilder().path("d:\\dev\\rdb_pictures\\test.rdb").password("password");

			try (RaccoonDatabase db = builder.get(DatabaseOpenOption.REPLACE))
			{
				for (File file : new File("d:\\dev\\rdb_pictures\\in\\").listFiles())
				{
					System.out.println(file);

					byte[] imageData = Files.readAllBytes(file.toPath());
					try (LobByteChannel lob = db.getDirectory("pics").open(ObjectId.randomId(), LobOpenOption.CREATE))
					{
						lob.getMetadata().put("width", 3200).put("height", 2400).putEpochTime("modified", file.lastModified()).put("name", file.getName());
						lob.writeAllBytes(imageData);
					}
				}

				db.commit();
			}

			try (RaccoonDatabase db = builder.get())
			{
				RaccoonDirectory collection = db.getDirectory("pics");

				collection.forEach((e,f)->System.out.println(f));

				db.getDirectory("pics").forEach((id, metadata) ->
				{
					System.out.println(metadata.toTypedJson());
					try (LobByteChannel lob = collection.open(id, LobOpenOption.READ))
					{
						lob.newInputStream().readAllBytes();
//						BufferedImage image = ImageIO.read(lob.newInputStream());
//						System.out.println(image);
					}
					catch (Exception e)
					{
						e.printStackTrace(System.out);
					}
				});
			}
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
	}
}
