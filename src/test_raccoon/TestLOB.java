package test_raccoon;

import java.awt.image.BufferedImage;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import javax.imageio.ImageIO;
import org.terifan.raccoon.RaccoonDatabase;
import org.terifan.raccoon.DatabaseOpenOption;
import org.terifan.raccoon.RaccoonCollection;
import org.terifan.raccoon.blockdevice.LobByteChannel;
import org.terifan.raccoon.blockdevice.LobOpenOption;
import org.terifan.raccoon.blockdevice.secure.AccessCredentials;
import org.terifan.raccoon.blockdevice.secure.CipherModeFunction;
import org.terifan.raccoon.blockdevice.secure.EncryptionFunction;
import org.terifan.raccoon.blockdevice.secure.KeyGenerationFunction;
import org.terifan.raccoon.document.Array;
import org.terifan.raccoon.document.Document;
import org.terifan.raccoon.document.ObjectId;


public class TestLOB
{
	public static void main(String... args)
	{
		try
		{
			AccessCredentials ac = new AccessCredentials("password".toCharArray(), EncryptionFunction.AES, KeyGenerationFunction.SHA3, CipherModeFunction.XTS);
//			AccessCredentials ac = null;

			try (RaccoonDatabase db = new RaccoonDatabase(Paths.get("d:\\test.rdb"), DatabaseOpenOption.REPLACE, ac))
			{
				RaccoonCollection filesCollection = db.getCollection("files");
				RaccoonCollection lobsCollection = db.getCollection("lobs");

				Files.walk(Paths.get("d:\\pictures")).filter(p -> p.getFileName().toString().toLowerCase().matches(".*jpg|.*png")).limit(10).forEach(path ->
				{
					System.out.println(path);

					try
					{
						Document file = new Document();
						file.put("_id", Array.of(path.getFileName().toString(), ObjectId.randomId()));
						file.put("length", Files.size(path));
						filesCollection.save(file);

						try (LobByteChannel lob = lobsCollection.openLob(file.get("_id"), LobOpenOption.CREATE))
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
				long t = System.currentTimeMillis();
				db.getCollection("files").listAll();
				System.out.println(System.currentTimeMillis()-t);

				RaccoonCollection lobCollection = db.getCollection("lobs");

				lobCollection.listAll().forEach(System.out::println);

				db.getCollection("files").listAll().forEach(file ->
				{
					try (LobByteChannel lob = lobCollection.openLob(file.get("_id"), LobOpenOption.READ))
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
}
