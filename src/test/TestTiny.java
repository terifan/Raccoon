package test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import org.terifan.raccoon.BTreeTableImplementation;
import org.terifan.raccoon.Database;
import org.terifan.raccoon.DatabaseOpenOption;
import org.terifan.raccoon.ScanResult;
import org.terifan.raccoon.annotations.Column;
import org.terifan.raccoon.annotations.Entity;
import org.terifan.raccoon.io.physical.MemoryBlockDevice;
import org.terifan.raccoon.annotations.Id;
import org.terifan.treegraph.HorizontalLayout;
import org.terifan.treegraph.TreeRenderer;
import org.terifan.treegraph.util.TextSlice;
import org.terifan.treegraph.util.VerticalImageFrame;


public class TestTiny
{
	private final static Random RND = new Random(1);

	private static VerticalImageFrame mTreeFrame;
	private static HashMap<String,String> mEntries;


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
//			for (;;)
			{
				mTreeFrame = new VerticalImageFrame();

				test();

//				mTreeFrame.getFrame().dispose();
			}
		}
		catch (Exception e)
		{
			e.printStackTrace(System.out);
		}
	}


	public static void test() throws Exception
	{
		mEntries = new HashMap<>();

		MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);

//		Log.setLevel(LogLevel.DEBUG);

		try (Database db = new Database(blockDevice, DatabaseOpenOption.CREATE_NEW))
		{
//			db.save(new Document().put("id",123).put("name","olle"));

//			insert(db, "Circus");
//			insert(db, "Banana");
//			insert(db, "Whale");
//			insert(db, "Xenon");
//			insert(db, "Open");
//			insert(db, "Rupee");
//			insert(db, "Silver");
//			insert(db, "Leap");
//			insert(db, "Ear");
//			insert(db, "Apple");
//			insert(db, "Yellow");
//			insert(db, "Turquoise");
//			insert(db, "Japanese");
//			insert(db, "Quality");
//			insert(db, "Nose");
//			insert(db, "Gloves");
//			insert(db, "Head");
//			insert(db, "Zebra");
//			insert(db, "Female");
//			insert(db, "Internal");
//			insert(db, "Jalapeno");
//			insert(db, "Urban");
//			insert(db, "Vapor");
//			insert(db, "Dove");
//			insert(db, "Mango");
//			insert(db, "Knife");
//
//			insert(db, "Clemens");
//			insert(db, "Bobby");
//			insert(db, "Wort");
//			insert(db, "Xor");
//			insert(db, "Order");
//			insert(db, "Ranger");
//			insert(db, "Surfing");
//			insert(db, "Love");
//			insert(db, "Eliot");
//			insert(db, "Asian");
//			insert(db, "Year");
//			insert(db, "Tank");
//			insert(db, "Jeans");
//			insert(db, "Queer");
//			insert(db, "Nickle");
//			insert(db, "Goat");
//			insert(db, "Happy");
//			insert(db, "Zink");
//			insert(db, "Furniture");
//			insert(db, "Immense");
//			insert(db, "Jehova");
//			insert(db, "Under");
//			insert(db, "Vital");
//			insert(db, "Dragon");
//			insert(db, "Many");
//			insert(db, "King");
//
//			insert(db, "Clemens");
//			insert(db, "Bread");
//			insert(db, "Wild");
//			insert(db, "Xanthe");
//			insert(db, "Opera");
//			insert(db, "River");
//			insert(db, "Sand");
//			insert(db, "Leach");
//			insert(db, "Electron");
//			insert(db, "Accuracy");
//			insert(db, "Yearning");
//			insert(db, "Tangent");
//			insert(db, "Jelly");
//			insert(db, "Queen");
//			insert(db, "Number");
//			insert(db, "Guts");
//			insert(db, "Harbor");
//			insert(db, "Zulu");
//			insert(db, "Fulfill");
//			insert(db, "Import");
//			insert(db, "Jupiter");
//			insert(db, "Ultra");
//			insert(db, "Voice");
//			insert(db, "Down");
//			insert(db, "Metal");
//			insert(db, "Knight");
//
//			insert(db, "Clear");
//			insert(db, "Breach");
//			insert(db, "Wilshire");
//			insert(db, "Xanthopsia");
//			insert(db, "Operation");
//			insert(db, "Robot");
//			insert(db, "Sugar");
//			insert(db, "Leather");
//			insert(db, "Ellipse");
//			insert(db, "Agree");
//			insert(db, "Yeisk");
//			insert(db, "Tartar");
//			insert(db, "Jigger");
//			insert(db, "Quelt");
//			insert(db, "Nutrition");
//			insert(db, "Gustus");
//			insert(db, "Hardner");
//			insert(db, "Zurvan");
//			insert(db, "Flead");
//			insert(db, "Instant");
//			insert(db, "Justis");
//			insert(db, "Umbrella");
//			insert(db, "Voltage");
//			insert(db, "Dwarf");
//			insert(db, "Misty");
//			insert(db, "Kart");
//
//			insert(db, "Christian");
//			insert(db, "Break");
//			insert(db, "Wilson");
//			insert(db, "Xanthoma");
//			insert(db, "Oven");
//			insert(db, "Rock");
//			insert(db, "Sudder");
//			insert(db, "Leap");
//			insert(db, "Eighty");
//			insert(db, "Alphabet");
//			insert(db, "Yekaterinburg");
//			insert(db, "Tassie");
//			insert(db, "Jewels");
//			insert(db, "Quernstone");
//			insert(db, "Nurses");
//			insert(db, "Goofer");
//			insert(db, "Hareem");
//			insert(db, "Zurek");
//			insert(db, "Flipper");
//			insert(db, "Intellectual");
//			insert(db, "Jitney");
//			insert(db, "Umbelled");
//			insert(db, "Vinyl");
//			insert(db, "Dwell");
//			insert(db, "Mold");
//			insert(db, "Karate");

			ArrayList<String> list = new ArrayList<>(Arrays.asList(
				"Circus", "Banana", "Whale", "Xenon", "Open", "Rupee", "Silver", "Leap", "Ear", "Apple", "Yellow", "Turquoise", "Japanese", "Quality", "Nose", "Gloves", "Head", "Zebra", "Female", "Internal", "Jalapeno", "Urban", "Vapor", "Dove", "Mango", "Knife",
				"Clemens", "Bobby", "Wort", "Xor", "Order", "Ranger", "Surfing", "Love", "Eliot", "Asian", "Year", "Tank", "Jeans", "Queer", "Nickle", "Goat", "Happy", "Zink", "Furniture", "Immense", "Jehova", "Under", "Vital", "Dragon", "Many", "King",
				"Clemens", "Bread", "Wild", "Xanthe", "Opera", "River", "Sand", "Leach", "Electron", "Accuracy", "Yearning", "Tangent", "Jelly", "Queen", "Number", "Guts", "Harbor", "Zulu", "Fulfill", "Import", "Jupiter", "Ultra", "Voice", "Down", "Metal", "Knight",
				"Clear", "Breach", "Wilshire", "Xanthopsia", "Operation", "Robot", "Sugar", "Leather", "Ellipse", "Agree", "Yeisk", "Tartar", "Jigger", "Quelt", "Nutrition", "Gustus", "Hardner", "Zurvan", "Flead", "Instant", "Justis", "Umbrella", "Voltage", "Dwarf", "Misty", "Kart",
				"Christian", "Break", "Wilson", "Xanthoma", "Oven", "Rock", "Sudder", "Leap", "Eighty", "Alphabet", "Yekaterinburg", "Tassie", "Jewels", "Quernstone", "Nurses", "Goofer", "Hareem", "Zurek", "Flipper", "Intellectual", "Jitney", "Umbelled", "Vinyl", "Dwell", "Mold", "Karate"
			));

			int seed = new Random().nextInt();
			System.out.println("seed=" + seed);
			Collections.shuffle(list, new Random(seed));

			for (String s : list)
			{
				insert(db, s);
			}

//			dump(db);

			boolean all = true;
			for (String key : mEntries.keySet())
			{
				try
				{
					all &= db.get(new KeyValue(key)).mValue.equals(mEntries.get(key));
				}
				catch (Exception e)
				{
					all = false;
					System.out.println("missing: " + key);
					e.printStackTrace(System.out);
					break;
				}
			}
			System.out.println(all ? "All keys found" : "Missing keys");

//			db.list(KeyValue.class).forEach(System.out::println);

			db.commit();
		}

//		blockDevice.dump();

		try (Database db = new Database(blockDevice, DatabaseOpenOption.READ_ONLY))
		{
//			db.list(KeyValue.class).forEach(System.out::print);

			boolean all = true;
			for (String key : mEntries.keySet())
			{
				try
				{
					all &= db.get(new KeyValue(key)) != null;
				}
				catch (Exception e)
				{
					System.out.println(key);
					e.printStackTrace(System.out);
					break;
				}
			}
			System.out.println(all ? "All keys found" : "Missing keys");
		}

//		try (Database db = new Database(blockDevice, DatabaseOpenOption.OPEN))
//		{
//			for (String key : new String[]{"Nose","Open","Quality"})
//			{
//				try
//				{
//					mTreeFrame.add(new TextSlice(key));
//					boolean removed = db.remove(new KeyValue(key));
////				if(!removed)throw new IllegalStateException();
//					if (BTreeTableImplementation.STOP) throw new IllegalStateException();
//				}
//				catch (Exception e)
//				{
//					System.out.println(key);
//					e.printStackTrace(System.out);
//				}
//				dump(db);
//			}
////		for (String key : new String[]{"Nose","Open","Quality","Rupee","Whale","Xenon","Yellow","Zebra"})
////		for (String key : new String[]{"Nose","Open","Quality","Rupee","Whale","Xenon","Yellow","Silver","Turquoise"})
//			for (String key : new String[]{"Nose","Open","Quality","Rupee","Whale","Xenon","Yellow","Apple","Banana","Circus","Ear","Female","Gloves","Zebra","Silver","Turquoise"})
//			{
//				try
//				{
//					mTreeFrame.add(new TextSlice(key));
//					boolean removed = db.remove(new KeyValue(key));
////				if(!removed)throw new IllegalStateException();
//					if (BTreeTableImplementation.STOP) throw new IllegalStateException();
//				}
//				catch (Exception e)
//				{
//					System.out.println(key);
//					e.printStackTrace(System.out);
//				}
//				dump(db);
//			}
//		}

		try (Database db = new Database(blockDevice, DatabaseOpenOption.OPEN))
		{
			int size = db.getTable(KeyValue.class).size();
			List<String> keys = new ArrayList<>(mEntries.keySet());
			int seed = new Random().nextInt();
			System.out.println("seed=" + seed);
			Collections.shuffle(keys, new Random(seed));
			for (String key : keys)
			{
				try
				{
					mTreeFrame.add(new TextSlice(key));
					boolean removed = db.remove(new KeyValue(key));
					dump(db);
					if(!removed)throw new IllegalStateException();
					if (BTreeTableImplementation.STOP) throw new IllegalStateException();
				}
				catch (Exception e)
				{
					System.out.println(key);
					e.printStackTrace(System.out);
					break;
				}

				if (db.getTable(KeyValue.class).size() != --size) throw new IllegalStateException("size: " + db.getTable(KeyValue.class).size() + ", expected: " + size);

//				if (key.equals("Apple")) throw new IllegalStateException();
			}
		}
	}


	private static void insert(Database aDatabase, String aKey) throws IOException
	{
		String value = Helper.createString(RND);

		mEntries.put(aKey, value);

		aDatabase.save(new KeyValue(aKey, value));
		dump(aDatabase);

		if (BTreeTableImplementation.STOP) throw new IllegalStateException();

//		if (rnd.nextInt(10) < 3)
//		{
//			mTreeFrame.add(new TextSlice("committing"));
//			aDatabase.commit();
//		}
	}


	private static void dump(Database aDatabase) throws IOException
	{
		String description = aDatabase.scan(new ScanResult()).getDescription();

		mTreeFrame.add(new TreeRenderer(description).render(new HorizontalLayout()));
	}


	@Entity(name = "KeyValue", implementation = "btree")
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
			return "[" + mKey + "=" + mValue + "]";
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
