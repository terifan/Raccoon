package test;

import java.awt.Dimension;
import java.io.FileInputStream;


public class TestBlobs2
{
//	public static void main(String... args)
//	{
//		try
//		{
//			MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);
//
//			try (Database db = new Database(blockDevice, DatabaseOpenOption.CREATE_NEW, CompressionParam.NO_COMPRESSION, new TableParam(1, 1)))
//			{
//				try (LobByteChannel channel = db.openLob(new PictureEntity("x", 1, "the name"), LobOpenOption.WRITE))
//				{
//					channel.writeAllBytes(new FileInputStream("d:\\desktop\\_sent_juncker.jpg"));
//				}
//
////				PictureEntity entity = new PictureEntity("cat", 0);
////				entity.name = "test1";
////				entity.thumb = new byte[10000];
////				db.save(entity);
//
//				db.commit();
//			}
//
////			blockDevice.dump();
//
//			try (Database db = new Database(blockDevice, DatabaseOpenOption.OPEN))
//			{
//				db.list(new PictureEntity("x")).forEach(entity->{
//					try
//					{
//						System.out.println("#" + entity.name);
//					}
//					catch (Exception e)
//					{
//						e.printStackTrace(System.out);
//					}
//				});
//			}
//		}
//		catch (Throwable e)
//		{
//			e.printStackTrace(System.out);
//		}
//	}
//
//
//	@Entity(name = "pictures")
//	static class PictureEntity
//	{
//		@Discriminator String category;
//		@Id Integer id;
//		@Column(name = "name") String name;
//		@Column(name = "size") Dimension dim;
//		@Column(name = "thumbnail") byte[] thumb;
//
//
//		public PictureEntity()
//		{
//		}
//
//
//		public PictureEntity(String aCategory)
//		{
//			category = aCategory;
//		}
//
//
//		public PictureEntity(String aCategory, Integer aId)
//		{
//			category = aCategory;
//			id = aId;
//		}
//
//
//		public PictureEntity(String aCategory, Integer aId, String aName)
//		{
//			this.category = aCategory;
//			this.id = aId;
//			this.name = aName;
//		}
//	}
}
