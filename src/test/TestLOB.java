package test;

import java.io.File;
import java.util.List;
import org.terifan.raccoon.RaccoonDatabase;
import org.terifan.raccoon.DatabaseOpenOption;
import org.terifan.raccoon.document.Array;
import org.terifan.raccoon.document.Document;
import org.terifan.raccoon.io.physical.MemoryBlockDevice;
import org.terifan.raccoon.io.secure.AccessCredentials;


public class TestLOB
{
	public static void main(String... args)
	{
		try
		{
			try (RaccoonDatabase db = new RaccoonDatabase(new MemoryBlockDevice(512), DatabaseOpenOption.CREATE, null))
			{
				for (int i = 0; i < 30; i++)
				{
					for (int j = 0; j < 5; j++)
					{
						db.getCollection("data").save(new Document().put("_id", Array.of(i, j)).put("text", i + "," + j + ":xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"));
					}
				}

				// in
				// exists
				// lt
				// lte
				// gt
				// gte
				// ne
				// nin +array
				// or +array
				// regex

				_Tools.showTree(db.getCollection("data")._getImplementation());

				Document x = db.getCollection("data").get(new Document().put("_id", Array.of(20, 2)));
				System.out.println(x);

				List<Document> lisx = db.getCollection("data").find(Document.of("_id:[{$gte:20,$lt:30},{$exists:true}]"));
				List<Document> list = db.getCollection("data").find(new Document().put("_id", Array.of(new Document().put("$gte",20).put("$lt",30), new Document().put("$exists", "true"))));

				list.forEach(e -> System.out.print(e.get("_id") + "\t"));
			}
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
	}


	public static void xmain(String... args)
	{
		try
		{
//			AccessCredentials ac = new AccessCredentials("password");
			AccessCredentials ac = null;

//			try ( RaccoonDatabase db = new RaccoonDatabase(new File("d:\\test.rdb"), DatabaseOpenOption.REPLACE, ac))
//			{
//				db.getCollection("folders").createIndex(new Document().put("ref", 1));
//
//				Files.walk(Paths.get("d:\\pictures")).filter(path -> Files.isRegularFile(path)).forEach(path ->
//				{
//					try
//					{
////						System.out.println(path);
//
//						ObjectId lobId = ObjectId.randomId();
//						try ( LobByteChannel lob = db.openLob(lobId, LobOpenOption.CREATE);  InputStream in = Files.newInputStream(path))
//						{
//							lob.writeAllBytes(in);
//						}
//
//						final AtomicReference<ObjectId> folderRef = new AtomicReference<>();
//						path.getParent().forEach(name ->
//						{
//							Document folder = new Document().put("_id", new Document().put("parent", folderRef.get()).put("name", name.toString()));
//							if (!db.getCollection("folders").tryGet(folder))
//							{
//								try
//								{
//									db.getCollection("folders").save(folder.put("ref", ObjectId.randomId()).put("modified", LocalDateTime.ofInstant(Files.getLastModifiedTime(path).toInstant(), ZoneId.systemDefault())));
//								}
//								catch (Exception e)
//								{
//									e.printStackTrace(System.out);
//								}
//							}
//							folderRef.set(folder.getObjectId("ref"));
//						});
//
//						db.getCollection("files").save(new Document().put("_id", new Document().put("folder", folderRef.get()).put("name", path.getFileName().toString())).put("lob", lobId).put("size", Files.size(path)).put("modified", LocalDateTime.ofInstant(Files.getLastModifiedTime(path).toInstant(), ZoneId.systemDefault())));
//					}
//					catch (Exception e)
//					{
//						e.printStackTrace(System.out);
//					}
//				});
//				db.commit();
//			}
			try (RaccoonDatabase db = new RaccoonDatabase(new File("d:\\test.rdb"), DatabaseOpenOption.OPEN, ac))
			{
//				db.getCollection("folders").stream().forEach(System.out::println);
//				db.getCollection("files").stream().forEach(System.out::println);

//				ObjectId ref = db.getCollection("folders").list().get(2).get("ref");
//				System.out.println("***"+ref);
//				System.out.println("--------");
//				Document d = db.getCollection("files").get(new Document().put("_id", new Document().put("folder", ref).put("name","kitty.jpg")));
//				System.out.println(d);
				Document root = db.getCollection("folders").get(new Document().put("_id", new Document().put("parent", null).put("name", "pictures")));
				Document folder = db.getCollection("folders").get(new Document().put("_id", new Document().put("parent", root.getObjectId("ref")).put("name", "Image Compression Suit")));

				System.out.println("--------");

				List<Document> list = db.getCollection("files").find(new Document().put("folder", folder.getObjectId("ref")));
				list.forEach(System.out::println);
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
