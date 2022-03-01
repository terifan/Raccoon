package test;

import java.awt.Dimension;
import java.io.IOException;
import java.nio.file.OpenOption;
import java.util.List;
import java.util.Random;
import org.terifan.raccoon.Database;
import org.terifan.raccoon.DatabaseOpenOption;
import org.terifan.raccoon.LogLevel;
import org.terifan.raccoon.ScanResult;
import org.terifan.raccoon.Table;
import org.terifan.raccoon.annotations.Discriminator;
import org.terifan.raccoon.annotations.Column;
import org.terifan.raccoon.annotations.Entity;
import org.terifan.raccoon.io.physical.MemoryBlockDevice;
import org.terifan.raccoon.annotations.Id;
import org.terifan.raccoon.docs.TreeRenderer;
import org.terifan.raccoon.serialization.FieldDescriptor;
import org.terifan.raccoon.util.Log;


public class TestTiny
{
	private static TreeRenderer mFrame = new TreeRenderer();


	public static void main(String... args)
	{
		try
		{
			Random rnd = new Random();
			MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);

//			Log.setLevel(LogLevel.INFO);

//

			try (Database db = new Database(blockDevice, DatabaseOpenOption.CREATE_NEW))
			{
//				db.save(new Document().put("id",123).put("name","olle"));

				db.save(new KeyValue("Apple", Helper.createString(rnd)));
				dump(db);
				db.save(new KeyValue("Banana", Helper.createString(rnd)));
				dump(db);
				db.save(new KeyValue("Circus", Helper.createString(rnd)));
				dump(db);
				db.save(new KeyValue("Dove", Helper.createString(rnd)));
				dump(db);
				db.save(new KeyValue("Dove", Helper.createString(rnd)));
				dump(db);
				db.save(new KeyValue("Ear", Helper.createString(rnd)));
				dump(db);
				db.save(new KeyValue("Female", Helper.createString(rnd)));
				dump(db);
				db.save(new KeyValue("Gloves", Helper.createString(rnd)));
				dump(db);
				db.save(new KeyValue("Head", Helper.createString(rnd)));
				dump(db);
				db.save(new KeyValue("Internal", Helper.createString(rnd)));
				dump(db);
				db.save(new KeyValue("Jalapeno", Helper.createString(rnd)));
				dump(db);
				db.save(new KeyValue("Japanese", Helper.createString(rnd)));
				dump(db);
				db.save(new KeyValue("Japanese", Helper.createString(rnd)));
				dump(db);
				db.save(new KeyValue("Knife", Helper.createString(rnd)));
				dump(db);
				db.save(new KeyValue("Leap", Helper.createString(rnd)));
				dump(db);
				db.save(new KeyValue("Mango", Helper.createString(rnd)));
				dump(db);
				db.save(new KeyValue("Nose", Helper.createString(rnd)));
				dump(db);
				db.save(new KeyValue("Open", Helper.createString(rnd)));
				dump(db);
				db.save(new KeyValue("Quality", Helper.createString(rnd)));
				dump(db);
				db.save(new KeyValue("Quality", Helper.createString(rnd)));
				dump(db);
				db.save(new KeyValue("Quality", Helper.createString(rnd)));
				dump(db);
				db.save(new KeyValue("Rupee", Helper.createString(rnd)));
				dump(db);

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
