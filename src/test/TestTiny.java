package test;

import java.io.IOException;
import java.util.Random;
import org.terifan.raccoon.ArrayMap;
import org.terifan.raccoon.ArrayMapEntry;
import org.terifan.raccoon.Database;
import org.terifan.raccoon.DatabaseOpenOption;
import org.terifan.raccoon.ScanResult;
import org.terifan.raccoon.annotations.Column;
import org.terifan.raccoon.annotations.Entity;
import org.terifan.raccoon.io.physical.MemoryBlockDevice;
import org.terifan.raccoon.annotations.Id;
import org.terifan.treegraph.TreeFrame;
import org.terifan.treegraph.TreeRenderer;
import org.terifan.treegraph.VerticalLayout;


public class TestTiny
{
	private static TreeFrame mTreeFrame = new TreeFrame();
	private static Random rnd = new Random(1);

//	private static ArrayMap arrayMap = new ArrayMap(1000);

	public static void main(String... args)
	{
		try
		{
			MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);

//			Log.setLevel(LogLevel.INFO);

			try (Database db = new Database(blockDevice, DatabaseOpenOption.CREATE_NEW))
			{
//				db.save(new Document().put("id",123).put("name","olle"));

//				insert(db, "a");
//				insert(db, "q");
//				insert(db, "l");
//				insert(db, "nnn");
//				insert(db, "x");
//				insert(db, "i");
//				insert(db, "j");
//				insert(db, "kkk");
//				insert(db, "m");
//				insert(db, "b");
//				insert(db, "ddd");
//				insert(db, "e");
//				insert(db, "f");
//				insert(db, "g");
//				insert(db, "r");
//				insert(db, "z");
//				insert(db, "y");
//				insert(db, "h");
//				insert(db, "w");
//				insert(db, "s");
//				insert(db, "t");
//				insert(db, "u");
//				insert(db, "v");
//				insert(db, "c");
//				insert(db, "p");
//				insert(db, "o");

				insert(db, "Circus");
				insert(db, "Banana");
				insert(db, "Whale");
				insert(db, "Xenon");
				insert(db, "Open");
				insert(db, "Rupee");
				insert(db, "Silver");
//				insert(db, "Leap");
//				insert(db, "Ear");
//				insert(db, "Apple");
//				insert(db, "Yellow");
//				insert(db, "Turquoise");
//				insert(db, "Japanese");
//				insert(db, "Quality");
//				insert(db, "Nose");
//				insert(db, "Gloves");
//				insert(db, "Head");
//				insert(db, "Zebra");
//				insert(db, "Female");
//				insert(db, "Internal");
//				insert(db, "Jalapeno");
//				insert(db, "Urban");
//				insert(db, "Vapor");
//				insert(db, "Dove");
//				insert(db, "Mango");
//				insert(db, "Knife");

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

		mTreeFrame.add(new TreeRenderer(description).render(new VerticalLayout()));
	}


	private static void insert(Database aDatabase, String aKey) throws IOException
	{
		String value = Helper.createString(rnd);

//		arrayMap.put(new ArrayMapEntry(aKey.getBytes(), value.getBytes(), (byte)0), null);
//		System.out.println("arrayMap=" + arrayMap);

		aDatabase.save(new KeyValue(aKey, value));
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
