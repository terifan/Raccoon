package test;


public class Test
{
//	public static void main(String... args)
//	{
//		try
//		{
//			long t = System.currentTimeMillis();
//
//			MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);
//
//			try (Database db = new Database(blockDevice, DatabaseOpenOption.CREATE_NEW, CompressionParam.NO_COMPRESSION, new TableParam(1, 1)))
//			{
//				for (int i = 1; i < 1000; i++)
//				{
//					db.save(new MyEntity(i, -i, "01234567890123456789"));
//				}
//
//				for (int i = 1; i < 100; i++)
//				{
//					db.remove(new MyEntity(i, -i, "01234567890123456789"));
//				}
//
//				db.commit();
//			}
//
//			System.out.println(System.currentTimeMillis() - t);
//
////			try (Database db = new Database(blockDevice, OpenOption.OPEN))
////			{
////				db.list(MyEntity.class).forEach(System.out::println);
////			}
//		}
//		catch (Throwable e)
//		{
//			e.printStackTrace(System.out);
//		}
//	}
//
//
//	@Entity(name = "entities")
//	static class MyEntity
//	{
//		@Discriminator String category;
//		@Id Integer id1;
//		@Id Integer id2;
//		@Column(name = "name") String name;
//		@Column(name = "size") Dimension dim;
//
//
//		public MyEntity()
//		{
//		}
//
//
//		public MyEntity(Integer aId1, Integer aId2, String aName)
//		{
//			id1 = aId1;
//			id2 = aId2;
//			name = aName;
//			dim = new Dimension(aId1, aId2);
//		}
//
//
//		@Override
//		public String toString()
//		{
//			return "MyEntity{" + "id1=" + id1 + ", id2=" + id2 + ", name=" + name + ", dim=" + dim + '}';
//		}
//	}
}
