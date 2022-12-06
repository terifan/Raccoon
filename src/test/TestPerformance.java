package test;

import java.io.File;


public class TestPerformance
{
//	public static void main(String... args)
//	{
//		try
//		{
//			for (int perLeaf = 1; perLeaf <= 8; perLeaf *= 2)
//			{
//				for (int perNode = 1; perNode <= 8; perNode *= 2)
//				{
//					for (int blockSize = 4096; blockSize <= 8192; blockSize *= 2)
//					{
//						long t = System.currentTimeMillis();
//
//						try (Database db = new DatabaseBuilder(new FileBlockDevice(new File("d:\\test_" + blockSize + "_" + perNode + "_" + perLeaf + ".db"), blockSize, false)).setPagesPerNode(perNode).setPagesPerLeaf(perLeaf).setCompression(CompressionParam.NO_COMPRESSION).create())
//						{
//							for (int i = 0; i < 1000_000; i++)
//							{
//								db.save(new MyEntity(i, "item-" + i));
//							}
//							db.commit();
//						}
//
//						System.out.print(blockSize + " " + perNode + " " + perLeaf + " " + (System.currentTimeMillis() - t) + "\t");
//					}
//
//					System.out.println();
//				}
//			}
//
////			try (Database db = new Database(new File("d:\\test.db"), OpenOption.OPEN, new AccessCredentials("password")))
////			{
////				db.list(MyEntity.class).forEach(System.out::println);
////			}
//		}
//		catch (Throwable e)
//		{
//			e.printStackTrace(System.out);
//		}
//	}
}
