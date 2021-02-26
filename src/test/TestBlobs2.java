package test;

import java.awt.Dimension;
import java.io.FileInputStream;
import java.io.InputStream;
import javax.imageio.ImageIO;
import org.terifan.raccoon.Blob;
import org.terifan.raccoon.LobOpenOption;
import org.terifan.raccoon.CompressionParam;
import org.terifan.raccoon.Database;
import org.terifan.raccoon.DatabaseOpenOption;
import org.terifan.raccoon.Parcel;
import org.terifan.raccoon.annotations.Discriminator;
import org.terifan.raccoon.TableParam;
import org.terifan.raccoon.annotations.Column;
import org.terifan.raccoon.annotations.Entity;
import org.terifan.raccoon.io.physical.MemoryBlockDevice;
import org.terifan.raccoon.annotations.Id;
import org.terifan.raccoon.LobByteChannel;


public class TestBlobs2
{
	public static void main(String... args)
	{
		try
		{
			MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);

			try (Database db = new Database(blockDevice, DatabaseOpenOption.CREATE_NEW, CompressionParam.NO_COMPRESSION, new TableParam(1, 1)))
			{
				byte[] thumbData = new byte[10000];

				PictureEntity picture = new PictureEntity(0);
				picture.category = "cat";
				picture.name = "test";
				picture.thumb = new Blob().setByteArray(thumbData);

				db.save(picture);

				try (LobByteChannel channel = db.openLob(picture, LobOpenOption.WRITE))
				{
					channel.writeAllBytes(new FileInputStream("file.jpg"));
				}

				picture.name = "test updated";
				db.save(picture);

				try (InputStream in = picture.image.newInputStream())
				{
				}

				db.commit();
			}

			try (Database db = new Database(blockDevice, DatabaseOpenOption.OPEN))
			{
				db.list(PictureEntity.class).forEach(pic->{
					try
					{
						ImageIO.read(pic.image.newInputStream());
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


	@Entity(name = "pictures")
	static class PictureEntity
	{
		@Discriminator String category;
		@Id Integer id;
		@Column(name = "name") String name;
		@Column(name = "size") Dimension dim;
		@Column(name = "thumbnail") Blob thumb;
		@Column(name = "image") Blob image;


		public PictureEntity()
		{
		}


		public PictureEntity(Integer aId)
		{
			id = aId;
		}
	}
}
