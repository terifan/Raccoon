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
		mStartTime = System.currentTimeMillis();

		try
		{
			MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);

//			ArrayList<String> list = WordLists.list78;
//			ArrayList<String> list = WordLists.list130;
//			ArrayList<String> list = WordLists.list502;
//			ArrayList<String> list = WordLists.list1007;
			ArrayList<String> list = WordLists.list4342;

			list = new ArrayList<>(list);
			Collections.shuffle(list, RND);

			try (Database db = new Database(blockDevice, DatabaseOpenOption.CREATE_NEW, CompressionParam.NO_COMPRESSION))
			{
				for (String key : list)
				{
					String value = Helper.createString(RND);
					mEntries.put(key, value);
					db.save(new _KeyValue(key, value));
					dump(db, key);
				}

				boolean all = true;
				for (String key : mEntries.keySet())
				{
					all &= db.get(new _KeyValue(key)).mValue.equals(mEntries.get(key));
				}
				if (!all) throw new Exception("Not all keys found");

				db.commit();
			}

			try (Database db = new Database(blockDevice, DatabaseOpenOption.OPEN))
			{
				boolean all = true;
				for (String key : mEntries.keySet())
				{
					all &= db.get(new _KeyValue(key)) != null;
				}
				if (!all) throw new Exception("Not all keys found");

				Collections.shuffle(list, RND);

				for (int i = list.size()/2; --i >= 0;)
				{
					String key = list.get(i);
					mEntries.remove(key);
					if(!db.remove(new _KeyValue(key))) throw new IllegalStateException("Failed to remove: " + key);
					dump(db, key);
				}

				db.commit();
			}

			try (Database db = new Database(blockDevice, DatabaseOpenOption.OPEN))
			{
				boolean all = true;
				for (String key : mEntries.keySet())
				{
					all &= db.get(new _KeyValue(key)) != null;
				}
				if (!all) throw new Exception("Not all keys found");

				Collections.shuffle(list, RND);

				for (int i = list.size()/2; --i >= 0;)
				{
					String key = list.get(i);
					String value = Helper.createString(RND);
					mEntries.put(key, value);
					db.save(new _KeyValue(key, value));
					dump(db, key);
				}

				db.commit();
			}

			try (Database db = new Database(blockDevice, DatabaseOpenOption.OPEN))
			{
				List<String> keys = new ArrayList<>(mEntries.keySet());
				Collections.shuffle(keys, RND);

				for (String key : keys)
				{
					mEntries.remove(key);
					if(!db.remove(new _KeyValue(key))) throw new IllegalStateException("Failed to remove: " + key);
					dump(db, key);
				}
			}
		}
		finally
		{
			mStopTime = System.currentTimeMillis();

			System.out.printf("#" + Console.BLUE + "%d" + Console.RESET + " time=" + Console.BLUE + "%s" + Console.RESET + " duration=" + Console.BLUE + "%s" + Console.RESET + " seed=" + Console.BLUE + "%s" + Console.RESET + "%n", ++TESTROUND, Helper.formatTime(System.currentTimeMillis() - mInitTime), Helper.formatTime(mStopTime - mStartTime), seed);
		}
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
