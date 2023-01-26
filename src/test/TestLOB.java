package test;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.ZoneId;
import javax.imageio.ImageIO;
import org.terifan.raccoon.RaccoonDatabase;
import org.terifan.raccoon.DatabaseOpenOption;
import org.terifan.raccoon.LobByteChannel;
import org.terifan.raccoon.LobOpenOption;
import org.terifan.raccoon.ObjectId;
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

			try ( RaccoonDatabase db = new RaccoonDatabase(new File("d:\\test.rdb"), DatabaseOpenOption.REPLACE, ac))
			{
				Files.walk(Paths.get("d:\\pictures")).filter(path -> Files.isRegularFile(path)).forEach(path ->
				{
					try
					{
						System.out.println(path);
						ObjectId fileId = ObjectId.randomId();
						try ( LobByteChannel lob = db.openLob(fileId, LobOpenOption.CREATE);  InputStream in = Files.newInputStream(path))
						{
							lob.writeAllBytes(in);
						}
						db.getCollection("files").save(new Document().put("lob", fileId).put("name", path.toString()).put("size", Files.size(path)).put("modified", LocalDateTime.ofInstant(Files.getLastModifiedTime(path).toInstant(), ZoneId.systemDefault())));
					}
					catch (Exception e)
					{
						e.printStackTrace(System.out);
					}
				});
				db.commit();
			}

			try ( RaccoonDatabase db = new RaccoonDatabase(new File("d:\\test.rdb"), DatabaseOpenOption.OPEN, ac))
			{
				db.getCollection("files").stream().forEach(file ->
				{
					try ( LobByteChannel lob = db.openLob(file.getObjectId("lob"), LobOpenOption.READ))
					{
						BufferedImage image = ImageIO.read(lob.newInputStream());
						System.out.println(image);
						lob.delete();
					}
					catch (Exception e)
					{
						e.printStackTrace(System.out);
					}
				});

				db.commit();
			}
		}
		catch (Exception e)
		{
			e.printStackTrace(System.out);
		}
	}
}
