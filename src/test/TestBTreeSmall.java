package test;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import javax.swing.JFrame;
import org.terifan.raccoon.BTreeTableImplementation;
import org.terifan.raccoon.CompressionParam;
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
	public static int TESTROUND;
	public static int TESTINDEX;

	private static VerticalImageFrame mTreeFrame;
	private static long mInitTime = System.currentTimeMillis();
	private static long mStartTime;
	private static long mStopTime;

	public Random RND;
	private HashMap<String,String> mEntries;
	private boolean mLog;


	public static void main(String... args)
	{
		try
		{
//			mTreeFrame = new VerticalImageFrame();
//			mTreeFrame.getFrame().setExtendedState(JFrame.MAXIMIZED_BOTH);

			for (;;)
			{
//				mTreeFrame = new VerticalImageFrame();

				new TestBTreeSmall().test();

//				mTreeFrame.getFrame().dispose();
			}
		}
		catch (Exception e)
		{
			e.printStackTrace(System.out);
		}
	}


	public void test() throws Exception
	{
		mEntries = new HashMap<>();

//		int seed = 2135994705;
		int seed = Math.abs(new Random().nextInt());
		RND = new Random(seed);

		mLog = true;

		System.out.println("#" + Console.BLUE + ++TESTROUND + Console.RESET + " time=" + Console.BLUE + Helper.formatTime(System.currentTimeMillis() - mInitTime) + Console.RESET + " duration=" + Console.BLUE + Helper.formatTime(mStopTime - mStartTime) + Console.RESET + " seed=" + Console.BLUE + seed + Console.RESET);

		mStartTime = System.currentTimeMillis();

		MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);

		try (Database db = new Database(blockDevice, DatabaseOpenOption.CREATE_NEW, CompressionParam.NO_COMPRESSION))
		{
//			ArrayList<String> list = WordLists.list78;
//			ArrayList<String> list = WordLists.list130;
//			ArrayList<String> list = WordLists.list502;
//			ArrayList<String> list = WordLists.list1007;
			ArrayList<String> list = WordLists.list4342;

			list = new ArrayList<>(list);
			Collections.shuffle(list, RND);

			for (String key : list)
			{
				String value = Helper.createString(RND);

//				System.out.println(CCC.BLUE + "Save " + key + CCC.RESET);

				mEntries.put(key, value);
				db.save(new _KeyValue(key, value));
				dump(db, key);

//				if (RND.nextInt(10) < 3)
//				{
//					mTreeFrame.add(new TextSlice("committing"));
//					db.commit();
//				}
			}

			boolean all = true;
			for (String key : mEntries.keySet())
			{
				try
				{
					all &= db.get(new _KeyValue(key)).mValue.equals(mEntries.get(key));
				}
				catch (Exception e)
				{
					all = false;
					System.out.println("missing: " + key);
					e.printStackTrace(System.out);
					break;
				}
			}
			if (!all) throw new Exception("Not all keys found");

			db.commit();
		}

		try (Database db = new Database(blockDevice, DatabaseOpenOption.READ_ONLY))
		{
			boolean all = true;
			for (String key : mEntries.keySet())
			{
				try
				{
					all &= db.get(new _KeyValue(key)) != null;
				}
				catch (Exception e)
				{
					System.out.println(key);
					e.printStackTrace(System.out);
					break;
				}
			}
			if (!all) throw new Exception("Not all keys found");
		}

		try (Database db = new Database(blockDevice, DatabaseOpenOption.OPEN))
		{
			List<String> keys = new ArrayList<>(mEntries.keySet());
			int size = keys.size();

			Collections.shuffle(keys, RND);

			for (String key : keys)
			{
//				mLog = TESTINDEX >= 6094;

				mEntries.remove(key);

//				System.out.println(CCC.BLUE + "Remove " + key + " " + TESTINDEX + CCC.RESET);

				try
				{
					boolean removed = db.remove(new _KeyValue(key));
					if(!removed)throw new IllegalStateException("Failed to remove: " + key);
				}
				catch (Exception e)
				{
					mLog = true;
					throw e;
				}
				finally
				{
					dump(db, key);
				}

				assert db.size(_KeyValue.class) == --size : "size missmatch: " + db.size(_KeyValue.class) + " != " + size;
			}
		}

		mStopTime = System.currentTimeMillis();
	}


	private void dump(Database aDatabase, String aKey) throws IOException
	{
		if (mLog && mTreeFrame != null)
		{
			String description = aDatabase.scan(new ScanResult()).getDescription();

			mTreeFrame.add(new TextSlice(TESTINDEX + " " + aKey));
			mTreeFrame.add(new TreeRenderer(description).render(new HorizontalLayout()));
		}

		TESTINDEX++;
	}
}
