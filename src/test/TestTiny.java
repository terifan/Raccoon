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
import org.terifan.treegraph.TreeRenderer;
import org.terifan.treegraph.VerticalLayout;
import org.terifan.treegraph.util.VerticalImageFrame;


public class TestTiny
{
	private static VerticalImageFrame mTreeFrame = new VerticalImageFrame();
	private static Random rnd = new Random(1);

//	private static ArrayMap arrayMap = new ArrayMap(1000);

//	public static void main(String ... args)
//	{
//		try
//		{
//			MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);
//
//			try (Database db = new Database(blockDevice, DatabaseOpenOption.CREATE_NEW))
//			{
//				String key = "alpha";
//				String value = Helper.createString(rnd);
//
//				db.save(new KeyValue(key, value));
//
//				KeyValue out = db.get(new KeyValue(key));
//
//				System.out.println(out);
//
//				dump(db);
//			}
//		}
//		catch (Throwable e)
//		{
//			e.printStackTrace(System.out);
//		}
//	}

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

				insertQ(db, "Circus");
				insertQ(db, "Banana");
				insertQ(db, "Whale");
				insertQ(db, "Xenon");
				insertQ(db, "Open");
				insertQ(db, "Rupee");
				insertQ(db, "Silver");
				insertQ(db, "Leap");
				insertQ(db, "Ear");
				insertQ(db, "Apple");
				insertQ(db, "Yellow");
				insertQ(db, "Turquoise");
				insertQ(db, "Japanese");
				insertQ(db, "Quality");
				insertQ(db, "Nose");
				insertQ(db, "Gloves");
				insertQ(db, "Head");
				insertQ(db, "Zebra");
				insertQ(db, "Female");
				insertQ(db, "Internal");
				insertQ(db, "Jalapeno");
				insertQ(db, "Urban");
				insertQ(db, "Vapor");
				insertQ(db, "Dove");
				insertQ(db, "Mango");
				insertQ(db, "Knife");

				insertQ(db, "Clemens");
				insertQ(db, "Bobby");
				insertQ(db, "Wort");
				insertQ(db, "Xor");
				insertQ(db, "Order");
				insertQ(db, "Ranger");
				insertQ(db, "Surfing");
				insertQ(db, "Love");
				insertQ(db, "Eliot");
				insertQ(db, "Asian");
				insertQ(db, "Year");
				insertQ(db, "Tank");
				insertQ(db, "Jeans");
				insertQ(db, "Queer");
				insertQ(db, "Nickle");
				insertQ(db, "Goat");
				insertQ(db, "Happy");
				insertQ(db, "Zink");
				insertQ(db, "Furniture");
				insertQ(db, "Immense");
				insertQ(db, "Jehova");
				insertQ(db, "Under");
				insertQ(db, "Vital");
				insertQ(db, "Dragon");
				insertQ(db, "Many");
				insertQ(db, "King");

				insertQ(db, "Clemens");
				insertQ(db, "Bread");
				insertQ(db, "Wild");
				insertQ(db, "Xanthe");
				insertQ(db, "Opera");
				insertQ(db, "River");
				insertQ(db, "Sand");
				insertQ(db, "Leach");
				insertQ(db, "Electron");
				insertQ(db, "Accuracy");
				insertQ(db, "Yearning");
				insertQ(db, "Tangent");
				insertQ(db, "Jelly");
				insertQ(db, "Queen");
				insertQ(db, "Number");
				insertQ(db, "Guts");
				insertQ(db, "Harbor");
				insertQ(db, "Zulu");
				insertQ(db, "Fulfill");
				insertQ(db, "Import");
				insertQ(db, "Jupiter");
				insertQ(db, "Ultra");
				insertQ(db, "Voice");
				insertQ(db, "Down");
				insertQ(db, "Metal");
				insertQ(db, "Knight");

				insertQ(db, "Clear");
				insertQ(db, "Breach");
				insertQ(db, "Wilshire");
				insertQ(db, "Xanthopsia");
				insertQ(db, "Operation");
				insertQ(db, "Robot");
				insertQ(db, "Sugar");
				insertQ(db, "Leather");
				insertQ(db, "Ellipse");
				insertQ(db, "Agree");
				insertQ(db, "Yeisk");
				insertQ(db, "Tartar");
				insertQ(db, "Jigger");
				insertQ(db, "Quelt");
				insertQ(db, "Nutrition");
				insertQ(db, "Gustus");
				insertQ(db, "Hardner");
				insertQ(db, "Zurvan");
				insertQ(db, "Flead");
				insertQ(db, "Instant");
				insertQ(db, "Justis");
				insertQ(db, "Umbrella");
				insertQ(db, "Voltage");
				insertQ(db, "Dwarf");
				insertQ(db, "Misty");
				insertQ(db, "Kart");

				insertQ(db, "Christian");
				insertQ(db, "Break");
				insertQ(db, "Wilson");
				insertQ(db, "Xanthoma");
				insertQ(db, "Oven");
				insertQ(db, "Rock");
				insertQ(db, "Sudder");
				insertQ(db, "Leap");
				insertQ(db, "Eighty");
				insertQ(db, "Alphabet");
				insertQ(db, "Yekaterinburg");
				insertQ(db, "Tassie");
				insertQ(db, "Jewels");
				insertQ(db, "Quernstone");
				insertQ(db, "Nurses");
				insertQ(db, "Goofer");
				insertQ(db, "Hareem");
				insertQ(db, "Zurek");
				insertQ(db, "Flipper");
				insertQ(db, "Intellectual");
				insertQ(db, "Jitney");
				insertQ(db, "Umbelled");
				insertQ(db, "Vinyl");
				insertQ(db, "Dwell");
				insertQ(db, "Mold");
				insertQ(db, "Karate");
				dump(db);

//				db.list(KeyValue.class).forEach(System.out::println);

				db.commit();
			}

//			blockDevice.dump();

//			System.out.println("-----------");

			try (Database db = new Database(blockDevice, DatabaseOpenOption.READ_ONLY))
			{
//				db.list(KeyValue.class).forEach(System.out::print);

				KeyValue value = db.get(new KeyValue("Goofer"));
				System.out.println(value);
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


	private static void insertQ(Database aDatabase, String aKey) throws IOException
	{
		String value = Helper.createString(rnd);

		aDatabase.save(new KeyValue(aKey, value));
	}


	@Entity(name = "keyvalue", implementation = "btree")
	public static class KeyValue
	{
		@Id(name="id", index = 0) String mKey;
		@Column(name="value") String mValue;

		public KeyValue()
		{
		}

		public KeyValue(String aKey)
		{
			mKey = aKey;
		}

		public KeyValue(String aKey, String aValue)
		{
			mKey = aKey;
			mValue = aValue;
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
