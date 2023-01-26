package test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
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
					lob.writeAllBytes(Files.readAllBytes(Paths.get("d:\\data2.txt")));
				}
				db.getCollection("files").save(new Document().put("file", id).put("_id", 1));
				db.commit();
			}

			try (RaccoonDatabase db = new RaccoonDatabase(new File("d:\\test.rdb"), DatabaseOpenOption.OPEN, ac))
			{
				Document doc = db.getCollection("files").get(new Document().put("_id", 1));
				byte[] data = db.getLob(doc.getObjectId("file"), LobOpenOption.READ).readAllBytes();

				System.out.println(doc);
				System.out.println(data.length);

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
