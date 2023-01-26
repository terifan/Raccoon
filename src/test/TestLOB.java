package test;

import java.awt.image.BufferedImage;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import javax.imageio.ImageIO;
import org.terifan.raccoon.RaccoonDatabase;
import org.terifan.raccoon.DatabaseOpenOption;
import org.terifan.raccoon.LobByteChannel;
import org.terifan.raccoon.LobOpenOption;
import org.terifan.raccoon.ObjectId;
import org.terifan.raccoon.RaccoonCollection;
import org.terifan.raccoon.document.Document;
import org.terifan.raccoon.io.secure.AccessCredentials;


public class TestLOB
{
	public static void main(String... args)
	{
		try
		{
//			AccessCredentials ac = new AccessCredentials("password");
			AccessCredentials ac = null;

			try (RaccoonDatabase db = new RaccoonDatabase(new File("d:\\test.rdb"), DatabaseOpenOption.REPLACE, ac))
			{
				ObjectId id = ObjectId.randomId();
				try (LobByteChannel lob = db.getLob(id, LobOpenOption.CREATE))
				{
					lob.writeAllBytes(Files.readAllBytes(Paths.get("d:\\pictures\\babe.jpg")));
				}
				db.getCollection("files").save(new Document().put("file", id).put("_id", 1));
				db.commit();
			}

			try (RaccoonDatabase db = new RaccoonDatabase(new File("d:\\test.rdb"), DatabaseOpenOption.OPEN, ac))
			{
				Document doc = db.getCollection("files").get(new Document().put("_id", 1));

				byte[] data;
				try (LobByteChannel lob = db.getLob(doc.getObjectId("file"), LobOpenOption.READ))
				{
					data = lob.readAllBytes();
				}

				System.out.println(doc);
				System.out.println(data.length);

				try (LobByteChannel lob = db.getLob(doc.getObjectId("file"), LobOpenOption.READ))
				{
					BufferedImage image = ImageIO.read(lob.newInputStream());
					System.out.println(image);
				}

				db.deleteLob(doc.getObjectId("file"));

				byte[] data2 = db.getLob(doc.getObjectId("file"), LobOpenOption.READ).readAllBytes();

				db.commit();
			}
		}
		catch (Exception e)
		{
			e.printStackTrace(System.out);
		}
	}
}
