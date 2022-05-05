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

	public static void main(String ... args)
	{
		try
		{
			MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);

			try (Database db = new Database(blockDevice, DatabaseOpenOption.CREATE_NEW))
			{
				String key = "alpha";
				String value = Helper.createString(rnd);

				db.save(new KeyValue(key, value));

				KeyValue out = db.get(new KeyValue(key));

				System.out.println(out);

				dump(db);
			}
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
	}

	public static void xmain(String... args)
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
				insert(db, "Leap");
				insert(db, "Ear");
				insert(db, "Apple");
				insert(db, "Yellow");
				insert(db, "Turquoise");
				insert(db, "Japanese");
				insert(db, "Quality");
				insert(db, "Nose");
				insert(db, "Gloves");
				insert(db, "Head");
				insert(db, "Zebra");
				insert(db, "Female");
				insert(db, "Internal");
				insert(db, "Jalapeno");
				insert(db, "Urban");
				insert(db, "Vapor");
				insert(db, "Dove");
				insert(db, "Mango");
				insert(db, "Knife");

				insert(db, "Clemens");
				insert(db, "Bobby");
				insert(db, "Wort");
				insert(db, "Xor");
				insert(db, "Order");
				insert(db, "Ranger");
				insert(db, "Surfing");
				insert(db, "Love");
				insert(db, "Eliot");
				insert(db, "Asian");
				insert(db, "Year");
				insert(db, "Tank");
				insert(db, "Jeans");
				insert(db, "Queer");
				insert(db, "Nickle");
				insert(db, "Goat");
				insert(db, "Happy");
				insert(db, "Zink");
				insert(db, "Furniture");
				insert(db, "Immense");
				insert(db, "Jehova");
				insert(db, "Under");
				insert(db, "Vital");
				insert(db, "Dragon");
				insert(db, "Many");
				insert(db, "King");

				insert(db, "Clemens");
				insert(db, "Bread");
				insert(db, "Wild");
				insert(db, "Xanthe");
				insert(db, "Opera");
				insert(db, "River");
				insert(db, "Sand");
				insert(db, "Leach");
				insert(db, "Electron");
				insert(db, "Accuracy");
				insert(db, "Yearning");
				insert(db, "Tangent");
				insert(db, "Jelly");
				insert(db, "Queen");
				insert(db, "Number");
				insert(db, "Guts");
				insert(db, "Harbor");
				insert(db, "Zulu");
				insert(db, "Fulfill");
				insert(db, "Import");
				insert(db, "Jupiter");
				insert(db, "Ultra");
				insert(db, "Voice");
				insert(db, "Down");
				insert(db, "Metal");
				insert(db, "Knight");

				insert(db, "Clear");
				insert(db, "Breach");
				insert(db, "Wilshire");
				insert(db, "Xanthopsia");
				insert(db, "Operation");
				insert(db, "Robot");
				insert(db, "Sugar");
				insert(db, "Leather");
				insert(db, "Ellipse");
				insert(db, "Agree");
				insert(db, "Yeisk");
				insert(db, "Tartar");
				insert(db, "Jigger");
				insert(db, "Quelt");
				insert(db, "Nutrition");
				insert(db, "Gustus");
				insert(db, "Hardner");
				insert(db, "Zurvan");
				insert(db, "Flead");
				insert(db, "Instant");
				insert(db, "Justis");
				insert(db, "Umbrella");
				insert(db, "Voltage");
				insert(db, "Dwarf");
				insert(db, "Misty");
				insert(db, "Kart");

				insert(db, "Christian");
				insert(db, "Break");
				insert(db, "Wilson");
				insert(db, "Xanthoma");
				insert(db, "Oven");
				insert(db, "Rock");
				insert(db, "Sudder");
				insert(db, "Leap");
				insert(db, "Eighty");
				insert(db, "Alphabet");
				insert(db, "Yekaterinburg");
				insert(db, "Tassie");
				insert(db, "Jewels");
				insert(db, "Quernstone");
				insert(db, "Nurses");
				insert(db, "Goofer");
				insert(db, "Hareem");
				insert(db, "Zurek");
				insert(db, "Flipper");
				insert(db, "Intellectual");
				insert(db, "Jitney");
				insert(db, "Umbelled");
				insert(db, "Vinyl");
				insert(db, "Dwell");
				insert(db, "Mold");
				insert(db, "Karate");

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
