package test;

import java.io.IOException;
import java.util.Random;
import org.terifan.raccoon.Database;
import org.terifan.raccoon.DatabaseOpenOption;
import org.terifan.raccoon.ScanResult;
import org.terifan.raccoon.annotations.Column;
import org.terifan.raccoon.annotations.Entity;
import org.terifan.raccoon.io.physical.MemoryBlockDevice;
import org.terifan.raccoon.annotations.Id;
import org.terifan.raccoon.docs.TreeRenderer;


public class TestTiny
{
	private static TreeRenderer mFrame = new TreeRenderer();
	private static Random rnd = new Random(1);


	public static void main(String... args)
	{
		try
		{
			MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);

//			Log.setLevel(LogLevel.INFO);

			try (Database db = new Database(blockDevice, DatabaseOpenOption.CREATE_NEW))
			{
//				db.save(new Document().put("id",123).put("name","olle"));

				insert(db, "Apple");
				insert(db, "Banana");
				insert(db, "Circus");
				insert(db, "Dove");
				insert(db, "Ear");
				insert(db, "Female");
				insert(db, "Gloves");
				insert(db, "Head");
				insert(db, "Internal");
				insert(db, "Jalapeno");
				insert(db, "Japanese");
				insert(db, "Knife");
				insert(db, "Leap");
				insert(db, "Mango");
				insert(db, "Nose");
				insert(db, "Open");
				insert(db, "Quality");
				insert(db, "Rupee");
				insert(db, "Silver");
				insert(db, "Turquoise");
				insert(db, "Urban");
				insert(db, "Vapor");
				insert(db, "Whale");
				insert(db, "Xenon");
				insert(db, "Yellow");
				insert(db, "Zebra");

//				db.list(KeyValue.class).forEach(System.out::println);

				db.commit();
			}

//			blockDevice.dump();

//			System.out.println("-----------");

			try (Database db = new Database(blockDevice, DatabaseOpenOption.READ_ONLY))
			{
//				db.list(KeyValue.class).forEach(System.out::print);
			}

//			System.out.println();

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


	private static void dump(Database aDatabase) throws IOException
	{
		String description = aDatabase.scan(new ScanResult()).getDescription();

		mFrame.add(mFrame.render(new TreeRenderer.VerticalLayout(), mFrame.parse(description)));
	}


	private static void insert(Database aDatabase, String aKey) throws IOException
	{
		aDatabase.save(new KeyValue(aKey, Helper.createString(rnd)));
		dump(aDatabase);
	}


	@Entity(name = "keyvalue", implementation = "btree")
	public static class KeyValue
	{
		@Id(name="id", index = 0) String mKey;
		@Column(name="value") String mValue;

		public KeyValue()
		{
		}

		public KeyValue(String aKey, String aValue)
		{
			this.mKey = aKey;
			this.mValue = aValue;
		}

		@Override
		public String toString()
		{
			return "[" + mKey + "]";
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
