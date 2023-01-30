package test;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.concurrent.atomic.AtomicReference;
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
				db.getCollection("folders").createIndex(new Document().put("ref", 1));

				Files.list(Paths.get("d:\\pictures")).filter(path -> Files.isRegularFile(path)).forEach(path ->
				{
					try
					{
						System.out.println(path);

						ObjectId lobId = ObjectId.randomId();
						try ( LobByteChannel lob = db.openLob(lobId, LobOpenOption.CREATE);  InputStream in = Files.newInputStream(path))
						{
							lob.writeAllBytes(in);
						}

						final AtomicReference<ObjectId> folderRef = new AtomicReference<>();
						path.getParent().forEach(name ->
						{
							Document folder = new Document().put("_id", new Document().put("parent", folderRef.get()).put("name", name.toString()));
							if (!db.getCollection("folders").tryGet(folder))
							{
								try
								{
									db.getCollection("folders").save(folder.put("ref", ObjectId.randomId()).put("modified", LocalDateTime.ofInstant(Files.getLastModifiedTime(path).toInstant(), ZoneId.systemDefault())));
								}
								catch (Exception e)
								{
									e.printStackTrace(System.out);
								}
							}
							folderRef.set(folder.getObjectId("ref"));
						});

						db.getCollection("files").save(new Document().put("_id", new Document().put("folder", folderRef.get()).put("name", path.getFileName().toString())).put("lob", lobId).put("size", Files.size(path)).put("modified", LocalDateTime.ofInstant(Files.getLastModifiedTime(path).toInstant(), ZoneId.systemDefault())));
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
				db.getCollection("folders").stream().forEach(System.out::println);
//				db.getCollection("files").stream().forEach(System.out::println);
			}

//			try ( RaccoonDatabase db = new RaccoonDatabase(new File("d:\\test.rdb"), DatabaseOpenOption.OPEN, ac))
//			{
//				db.getCollection("files").stream().forEach(file ->
//				{
//					try ( LobByteChannel lob = db.openLob(file.getObjectId("lob"), LobOpenOption.READ))
//					{
//						BufferedImage image = ImageIO.read(lob.newInputStream());
//						System.out.println(image);
//						lob.delete();
//					}
//					catch (Exception e)
//					{
//						e.printStackTrace(System.out);
//					}
//				});
//
//				db.commit();
//			}
		}
		catch (Exception e)
		{
			e.printStackTrace(System.out);
		}
	}
}
