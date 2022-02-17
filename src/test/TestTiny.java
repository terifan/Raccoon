package test;

import java.awt.Dimension;
import java.nio.file.OpenOption;
import java.util.List;
import java.util.Random;
import org.terifan.raccoon.Database;
import org.terifan.raccoon.DatabaseOpenOption;
import org.terifan.raccoon.LogLevel;
import org.terifan.raccoon.Table;
import org.terifan.raccoon.annotations.Discriminator;
import org.terifan.raccoon.annotations.Column;
import org.terifan.raccoon.annotations.Entity;
import org.terifan.raccoon.io.physical.MemoryBlockDevice;
import org.terifan.raccoon.annotations.Id;
import org.terifan.raccoon.serialization.FieldDescriptor;
import org.terifan.raccoon.util.Log;


public class TestTiny
{
	public static void main(String... args)
	{
		try
		{
			MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);

//			Log.setLevel(LogLevel.INFO);

			try (Database db = new Database(blockDevice, DatabaseOpenOption.CREATE_NEW))
			{
//				db.save(new Document().put("id",123).put("name","olle"));

				db.save(new KeyValue("papaya", Helper.createString(new Random(1), 80)));
				db.save(new KeyValue("olives", Helper.createString(new Random(1), 80)));
				db.save(new KeyValue("orange", Helper.createString(new Random(1), 80)));
				db.save(new KeyValue("banana", Helper.createString(new Random(1), 80)));
				db.save(new KeyValue("cherry", Helper.createString(new Random(1), 80)));
				db.list(KeyValue.class).forEach(System.out::println);
				db.commit();
			}

//			blockDevice.dump();

			System.out.println("-----------");

			try (Database db = new Database(blockDevice, DatabaseOpenOption.READ_ONLY))
			{
				db.list(KeyValue.class).forEach(System.out::println);
			}

//			try (Database db = new Database(blockDevice, DatabaseOpenOption.CREATE))
//			{
//				Fruit apple = new Fruit(1L);
//				db.get(apple);
//				apple.mWeight = 1.1;
//				db.save(apple);
//
//				db.commit();
//			}
//
//			try (Database db = new Database(blockDevice, DatabaseOpenOption.READ_ONLY))
//			{
//				System.out.println("-------- -------- -------- -------- -------- -------- -------- -------- -------- --------");
//
//				for (Table table : db.getTables())
//				{
//					System.out.println(table.getEntityName());
//					for (FieldDescriptor f : table.getFields())
//					{
//						System.out.println("\t" + f);
//					}
//				}
//
//				System.out.println("-------- -------- -------- -------- -------- -------- -------- -------- -------- --------");
//
//				db.list(Fruit.class).forEach(System.out::println);
//			}
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
	}


	@Entity(name = "keyvalue", implementation = "btree")
	public static class KeyValue
	{
		@Id(name="id") String mKey;
		@Column(name="value") String mValue;

		public KeyValue()
		{
		}

		public KeyValue(String aKey, String aValue)
		{
			this.mKey = aKey;
			this.mValue = aValue;
		}
	}


	@Entity(name = "fruits", implementation = "btree")
	public static class Fruit
	{
		@Column(name = "id") Long mId;
		@Id String mName;
		@Column(name = "weight") double mWeight;
		@Column(name = "cost") Double mCost;
//		@Column(name = "size") Dimension mDimension;
		@Column(name = "x") int[][] x;


		public Fruit()
		{
		}


		public Fruit(Long aId)
		{
			mId = aId;
		}


		public Fruit(String aName, double aWeight)
		{
			mId = System.nanoTime();
			mName = aName;
			mWeight = aWeight;
//			mDimension = new Dimension(new Random().nextInt(100), new Random().nextInt(100));
		}


		@Override
		public String toString()
		{
//			return "MyEntity{" + "id=" + mId + ", name=" + mName + ", weight=" + mWeight + ", dim=" + mDimension + '}';
			return "MyEntity{" + "id=" + mId + ", name=" + mName + ", weight=" + mWeight + '}';
		}
	}
}
