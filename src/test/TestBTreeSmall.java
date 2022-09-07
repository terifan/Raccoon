package test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import javax.swing.JFrame;
import org.terifan.raccoon.BTreeIndex;
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


public class TestBTreeSmall
{
	public static Random RND;

	private static VerticalImageFrame mTreeFrame;
	private static HashMap<String,String> mEntries;

	private static boolean mLog = !true;


	public static void main(String... args)
	{
		try
		{
//			mTreeFrame = new VerticalImageFrame();
//			mTreeFrame.getFrame().setExtendedState(JFrame.MAXIMIZED_BOTH);

			for (;;)
			{
//				mTreeFrame = new VerticalImageFrame();

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

		BTreeIndex.op = 0;
		BTreeTableImplementation.TESTINDEX = 0;

//		int seed = -383991152;
		int seed = new Random().nextInt();
		RND = new Random(seed);

		MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);

		try (Database db = new Database(blockDevice, DatabaseOpenOption.CREATE_NEW))
		{
//			ArrayList<String> list = WordLists.list78;
//			ArrayList<String> list = WordLists.list505;
//			ArrayList<String> list = WordLists.list1007;
			ArrayList<String> list = WordLists.list4342;

			System.out.println("seed=" + seed);

			list = new ArrayList<>(list);
			Collections.shuffle(list, RND);

			for (String s : list)
			{
				insert(db, s);
			}

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

			db.commit();
		}

		try (Database db = new Database(blockDevice, DatabaseOpenOption.READ_ONLY))
		{
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

		try (Database db = new Database(blockDevice, DatabaseOpenOption.OPEN))
		{
			dump(db, "x");

			int size = db.getTable(KeyValue.class).size();
			List<String> keys = new ArrayList<>(mEntries.keySet());

			Collections.shuffle(keys, RND);

			for (String key : keys)
			{
//				mLog = BTreeTableImplementation.TESTINDEX > 1630;

				mEntries.remove(key);
//				System.out.println(ConsoleColorCodes.BLUE + "Remove " + key + ConsoleColorCodes.RESET);

				try
				{
					boolean removed = db.remove(new KeyValue(key));
					if(!removed)throw new IllegalStateException(key);
					if (BTreeTableImplementation.STOP)
					{
						dump(db, "<stopped>");
						throw new IllegalStateException();
					}
				}
//				catch (Exception e)
//				{
//					System.out.println(key);
//					e.printStackTrace(System.out);
//					break;
//				}
				finally
				{
					dump(db, key);
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

		dump(aDatabase, aKey);

		if (BTreeTableImplementation.STOP) throw new IllegalStateException();

//		if (rnd.nextInt(10) < 3)
//		{
//			mTreeFrame.add(new TextSlice("committing"));
//			aDatabase.commit();
//		}
	}


	private static void dump(Database aDatabase, String aKey) throws IOException
	{
		if (mLog && mTreeFrame != null)
		{
			String description = aDatabase.scan(new ScanResult()).getDescription();

			mTreeFrame.add(new TextSlice(BTreeTableImplementation.TESTINDEX + " " + aKey));
			mTreeFrame.add(new TreeRenderer(description).render(new HorizontalLayout()));
		}

		BTreeTableImplementation.TESTINDEX++;
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
}
