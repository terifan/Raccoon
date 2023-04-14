package test;

import java.awt.image.BufferedImage;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import javax.imageio.ImageIO;
import org.terifan.raccoon.RaccoonDatabase;
import org.terifan.raccoon.DatabaseOpenOption;
import org.terifan.raccoon.io.LobByteChannel;
import org.terifan.raccoon.io.LobOpenOption;
import org.terifan.raccoon.RaccoonCollection;
import org.terifan.raccoon.document.Document;
import org.terifan.raccoon.document.ObjectId;
import org.terifan.raccoon.io.secure.AccessCredentials;
import org.terifan.raccoon.io.secure.CipherModeFunction;
import org.terifan.raccoon.io.secure.EncryptionFunction;
import org.terifan.raccoon.io.secure.KeyGenerationFunction;


public class TestLOB
{
	public static void main(String... args)
	{
		try
		{
			AccessCredentials ac = new AccessCredentials("password".toCharArray(), EncryptionFunction.AES_TWOFISH_SERPENT, KeyGenerationFunction.SHA3, CipherModeFunction.XTS);
//			AccessCredentials ac = null;

			try (RaccoonDatabase db = new RaccoonDatabase(Paths.get("d:\\test.rdb"), DatabaseOpenOption.REPLACE, ac))
			{
				RaccoonCollection lobs = db.getCollection("files");

				Files.walk(Paths.get("d:\\pictures")).filter(p -> p.getFileName().toString().toLowerCase().matches(".*jpg|.*png")).limit(20).forEach(path ->
				{
					System.out.println(path);

					try
					{
						Document doc = new Document();
						doc.put("_id", ObjectId.randomId());
						doc.put("name", path.toString());
						doc.put("length", Files.size(path));
						lobs.save(doc);

						try (LobByteChannel lob = db.openLob(doc.getObjectId("_id"), LobOpenOption.CREATE))
						{
							try (InputStream in = Files.newInputStream(path))
							{
								lob.writeAllBytes(in);
							}
						}
					}
					catch (Exception e)
					{
						e.printStackTrace(System.out);
					}
				});

				db.commit();
			}

			try (RaccoonDatabase db = new RaccoonDatabase(Paths.get("d:\\test.rdb"), DatabaseOpenOption.OPEN, ac))
			{
				db.getCollection("files").listAll().forEach(file ->
				{
					try ( LobByteChannel lob = db.openLob(file.getObjectId("_id"), LobOpenOption.READ))
					{
						BufferedImage image = ImageIO.read(lob.newInputStream());
						System.out.println(image);
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


//	public static void xmain(String... args)
//	{
//		try
//		{
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
//			try (RaccoonDatabase db = new RaccoonDatabase(new File("d:\\test.rdb"), DatabaseOpenOption.OPEN, ac))
//			{
//				db.getCollection("folders").stream().forEach(System.out::println);
//				db.getCollection("files").stream().forEach(System.out::println);

//				ObjectId ref = db.getCollection("folders").list().get(2).get("ref");
//				System.out.println("***"+ref);
//				System.out.println("--------");
//				Document d = db.getCollection("files").get(new Document().put("_id", new Document().put("folder", ref).put("name","kitty.jpg")));
//				System.out.println(d);
//				Document root = db.getCollection("folders").get(new Document().put("_id", new Document().put("parent", null).put("name", "pictures")));
//				Document folder = db.getCollection("folders").get(new Document().put("_id", new Document().put("parent", root.getObjectId("ref")).put("name", "Image Compression Suit")));
//
//				System.out.println("--------");
//
//				List<Document> list = db.getCollection("files").find(new Document().put("folder", folder.getObjectId("ref")));
//				list.forEach(System.out::println);
//			}
//
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
//		}
//		catch (Exception e)
//		{
//			e.printStackTrace(System.out);
//		}
//	}
}
