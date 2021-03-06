package test;

import java.awt.Dimension;
import java.nio.file.OpenOption;
import java.util.List;
import java.util.Random;
import org.terifan.raccoon.Database;
import org.terifan.raccoon.DatabaseOpenOption;
import org.terifan.raccoon.Table;
import org.terifan.raccoon.annotations.Discriminator;
import org.terifan.raccoon.annotations.Column;
import org.terifan.raccoon.annotations.Entity;
import org.terifan.raccoon.io.physical.MemoryBlockDevice;
import org.terifan.raccoon.annotations.Id;
import org.terifan.raccoon.serialization.FieldDescriptor;


public class TestTiny
{
	public static void main(String... args)
	{
		try
		{
			MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);

			try (Database db = new Database(blockDevice, DatabaseOpenOption.CREATE_NEW))
			{
				db.save(new Fruit(1, "apple", 1.4));
				db.save(new Fruit(2, "banana", 2.1));
				db.save(new Fruit(3, "lemon", 3.2));
				db.commit();
			}

			try (Database db = new Database(blockDevice, DatabaseOpenOption.READ_ONLY))
			{
				db.list(Fruit.class).forEach(System.out::println);
			}

			try (Database db = new Database(blockDevice, DatabaseOpenOption.CREATE))
			{
				Fruit apple = new Fruit(1);
				db.get(apple);
				apple.mWeight = 1.1;
				db.save(apple);

				db.commit();
			}

			try (Database db = new Database(blockDevice, DatabaseOpenOption.READ_ONLY))
			{
				System.out.println("-------- -------- -------- -------- -------- -------- -------- -------- -------- --------");

				for (Table table : db.getTables())
				{
					System.out.println(table.getEntityName());
					for (FieldDescriptor f : table.getFields())
					{
						System.out.println("\t" + f);
					}
				}

				System.out.println("-------- -------- -------- -------- -------- -------- -------- -------- -------- --------");

				db.list(Fruit.class).forEach(System.out::println);
			}
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
	}


	@Entity(name = "fruits")
	public static class Fruit
	{
		@Id Integer mId;
		@Column(name = "name") String mName;
		@Column(name = "weight") double mWeight;
		@Column(name = "cost") Double mCost;
		@Column(name = "size") Dimension mDimension;
		@Column(name = "x") int[][] x;


		public Fruit()
		{
		}


		public Fruit(Integer aId)
		{
			mId = aId;
		}


		public Fruit(Integer aId, String aName, double aWeight)
		{
			mId = aId;
			mName = aName;
			mWeight = aWeight;
			mDimension = new Dimension(new Random().nextInt(100), new Random().nextInt(100));
		}


		@Override
		public String toString()
		{
			return "MyEntity{" + "id=" + mId + ", name=" + mName + ", weight=" + mWeight + ", dim=" + mDimension + '}';
		}
	}
}
