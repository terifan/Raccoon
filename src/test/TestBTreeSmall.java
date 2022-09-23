package test;

import java.awt.Color;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import org.terifan.raccoon.CompressionParam;
import org.terifan.raccoon.Database;
import org.terifan.raccoon.DatabaseOpenOption;
import org.terifan.raccoon.ScanResult;
import org.terifan.raccoon.io.managed.ManagedBlockDevice;
import org.terifan.raccoon.io.managed.RangeMap;
import org.terifan.raccoon.io.physical.MemoryBlockDevice;
import org.terifan.treegraph.HorizontalLayout;
import org.terifan.treegraph.TreeRenderer;
import org.terifan.treegraph.util.TextSlice;
import org.terifan.treegraph.util.VerticalImageFrame;


// https://www.tutorialspoint.com/mongodb/mongodb_java.htm


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

//			for (;;)
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

		int seed = 1131648982;
//		int seed = Math.abs(new Random().nextInt());
		RND = new Random(seed);

		System.out.println(seed);

		mLog = true;
		mStartTime = System.currentTimeMillis();

		try
		{
			MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);

//			ArrayList<String> list = WordLists.list78;
//			ArrayList<String> list = WordLists.list130;
//			ArrayList<String> list = WordLists.list502;
			ArrayList<String> list = WordLists.list1007;
//			ArrayList<String> list = WordLists.list4342;

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

					commit(db, 0);
				}

				boolean all = true;
				for (String key : mEntries.keySet())
				{
					all &= db.get(new _KeyValue(key)).mValue.equals(mEntries.get(key));
				}
				if (!all) throw new Exception("Not all keys found");

				commit(db, 10);

//				ScanResult result = db.scan(new ScanResult());
//				System.out.println(result);
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

					commit(db, 0);
				}

				commit(db, 10);
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

					commit(db, 0);
				}

				commit(db, 10);
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

					commit(db, 0);
				}
			}
		}
		finally
		{
			mStopTime = System.currentTimeMillis();

			System.out.printf("#" + Console.BLUE + "%d" + Console.RESET + " time=" + Console.BLUE + "%s" + Console.RESET + " duration=" + Console.BLUE + "%s" + Console.RESET + " seed=" + Console.BLUE + "%s" + Console.RESET + "%n", ++TESTROUND, Helper.formatTime(System.currentTimeMillis() - mInitTime), Helper.formatTime(mStopTime - mStartTime), seed);
		}
	}


	private void commit(Database aDatabase, int aProb)
	{
		if (RND.nextInt(10) <= aProb)
		{
			if (mTreeFrame != null)
			{
				mTreeFrame.add(new TextSlice("Commit", Color.GREEN, Color.WHITE, 10));
			}

			aDatabase.commit();

//			long alloc = aDatabase.getBlockDevice().getAllocatedSpace() / 10;
//			long used = aDatabase.getBlockDevice().getUsedSpace() / 10;
//
//			RangeMap rangeMap = ((ManagedBlockDevice)aDatabase.getBlockDevice()).getRangeMap();
//
//			for (int i = 0; i < alloc; i++)
//			{
//				boolean f = true;
//				for (int j = 0; j < 10; j++) f &= rangeMap.isFree(10*i+j, 1);
//
//				System.out.print(i < used ? f?"+":"*" : "-");
//			}
//			System.out.println();

//			System.out.printf("%5d %5d %5d%n", aDatabase.getBlockDevice().getUsedSpace(), free, alloc);
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
