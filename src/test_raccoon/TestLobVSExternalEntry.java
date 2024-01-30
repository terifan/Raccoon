package test_raccoon;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import org.terifan.raccoon.RaccoonDatabase;
import org.terifan.raccoon.DatabaseOpenOption;
import org.terifan.raccoon.RaccoonBuilder;
import org.terifan.raccoon.RaccoonCollection;
import org.terifan.raccoon.RaccoonDirectory;
import org.terifan.raccoon.blockdevice.LobByteChannel;
import org.terifan.raccoon.blockdevice.LobOpenOption;
import org.terifan.raccoon.document.Document;
import org.terifan.raccoon.document.ObjectId;


public class TestLobVSExternalEntry
{
	public static void main(String... args)
	{
		try
		{
			RaccoonBuilder builder = new RaccoonBuilder().path("d:\\dev\\rdb_pictures\\test.rdb").compressor("none");

			try (RaccoonDatabase db = builder.get(DatabaseOpenOption.REPLACE))
			{
				File[] files = new File("d:\\dev\\rdb_pictures\\in\\").listFiles();

				long col0 = System.currentTimeMillis();
				for (File file : files)
				{
					byte[] imageData = Files.readAllBytes(file.toPath());
					db.getCollection("pics-col").save(new Document().put("data", imageData).put("name", file.getName()));
				}
				db.commit();
				long col1 = System.currentTimeMillis();

				long lob0 = System.currentTimeMillis();
				for (File file : files)
				{
					byte[] imageData = Files.readAllBytes(file.toPath());
					db.getDirectory("pics-lob").writeAllBytes(ObjectId.randomId(), imageData, new Document().put("name", file.getName()));

//					try (LobByteChannel channel = db.getDirectory("pics-lob").open(ObjectId.randomId(), LobOpenOption.CREATE))
//					{
//						channel.getMetadata().put("name", file.getName());
//						channel.writeAllBytes(imageData);
//					}
				}
				db.commit();
				long lob1 = System.currentTimeMillis();

				db.getDirectory("other").writeAllBytes(ObjectId.randomId(), "test".getBytes(), Document.of("a:1"));
				db.commit();

				System.out.println("lob: " + (lob1 - lob0));
				System.out.println("col: " + (col1 - col0));
			}

			try (RaccoonDatabase db = builder.get())
			{
				long col0 = System.currentTimeMillis();
				RaccoonCollection collection = db.getCollection("pics-col");
				collection.forEach(doc -> {});
//				collection.forEach(doc -> System.out.println(doc.getString("name")));
				long col1 = System.currentTimeMillis();

				long lob0 = System.currentTimeMillis();
				RaccoonDirectory dir = db.getDirectory("pics-lob");
				dir.forEach((lob,doc) -> {});
//				dir.forEach((lob,doc) -> System.out.println(doc.getString("name")));
				long lob1 = System.currentTimeMillis();

				Document metadata = new Document();
				ObjectId id = (ObjectId)db.getDirectory("other").list().getFirst();
				System.out.println(db.getDirectory("other").readAllBytes(id, metadata).length);
				System.out.println(metadata);

				System.out.println("lob: " + (lob1 - lob0));
				System.out.println("col: " + (col1 - col0));
			}

			try (RaccoonDatabase db = builder.get())
			{
				long col0 = System.currentTimeMillis();
				RaccoonCollection collection = db.getCollection("pics-col");
				collection.forEach(doc -> doc.getBinary("data"));
				long col1 = System.currentTimeMillis();

				long lob0 = System.currentTimeMillis();
				RaccoonDirectory dir = db.getDirectory("pics-lob");
				dir.forEach((lob,doc) -> dir.readAllBytes(lob));
				long lob1 = System.currentTimeMillis();

				System.out.println("lob: " + (lob1 - lob0));
				System.out.println("col: " + (col1 - col0));
			}
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
	}
}
