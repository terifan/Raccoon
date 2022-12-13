package test;

import java.awt.Color;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import org.terifan.raccoon.RaccoonDatabase;
import org.terifan.raccoon.DatabaseOpenOption;
import org.terifan.raccoon.ScanResult;
import org.terifan.raccoon.io.managed.ManagedBlockDevice;
import org.terifan.raccoon.io.managed.RangeMap;
import org.terifan.raccoon.io.physical.MemoryBlockDevice;
import org.terifan.raccoon.monitoring.DatabaseMonitorWindow;
import org.terifan.raccoon.monitoring.MonitorInstance;
import org.terifan.raccoon.util.Console;
import org.terifan.treegraph.HorizontalLayout;
import org.terifan.treegraph.TreeGraph;
import org.terifan.treegraph.util.TextSlice;
import org.terifan.treegraph.util.VerticalImageFrame;


public class TestBTreeSmall
{
//	public static int TESTROUND;
//	public static int TESTINDEX;
//
//	private static VerticalImageFrame mTreeFrame;
//	private static long mInitTime = System.currentTimeMillis();
//	private static long mStartTime;
//	private static long mStopTime;
//
//	public Random RND;
//	private HashMap<String,String> mEntries;
//	private boolean mLog;
//
//	private int COMMIT;
//
//	private static DatabaseMonitorWindow mDatabaseMonitorWindow;
//
//
//	public static void main(String... args)
//	{
//		try
//		{
//			mDatabaseMonitorWindow = new DatabaseMonitorWindow();
//
////			mTreeFrame = new VerticalImageFrame();
////			mTreeFrame.getFrame().setExtendedState(JFrame.MAXIMIZED_BOTH);
//
////			for (;;)
//			{
////				mTreeFrame = new VerticalImageFrame();
//
//				new TestBTreeSmall().test();
//
////				mTreeFrame.getFrame().dispose();
//			}
//		}
//		catch (Exception e)
//		{
//			e.printStackTrace(System.out);
//		}
//	}
//
//
//	public void test() throws Exception
//	{
//		mEntries = new HashMap<>();
//
////		int seed = 1838291525;
//		int seed = Math.abs(new Random().nextInt());
//		RND = new Random(seed);
//
//		mLog = true;
//		mStartTime = System.currentTimeMillis();
//
//		int FETCH = 0;
//		int INSERT = 0;
//		int UPDATE = 0;
//		int DELETE = 0;
//		COMMIT = 0;
//		int COMMIT_FREQ = 1;
//
//		try
//		{
//			MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);
//
////			ArrayList<String> list = WordLists.list78;
////			ArrayList<String> list = WordLists.list130;
////			ArrayList<String> list = WordLists.list502;
////			ArrayList<String> list = WordLists.list1007;
//			ArrayList<String> list = WordLists.list4342;
//
//			list = new ArrayList<>(list);
//			Collections.shuffle(list, RND);
//
//			try (Database db = new Database(blockDevice, DatabaseOpenOption.CREATE_NEW); MonitorInstance mi = mDatabaseMonitorWindow.attach(db))
//			{
//				for (String key : list)
//				{
//					String value = Helper.createString(RND);
//					mEntries.put(key, value);
//					if (db.save(new _KeyValue(key, value))) UPDATE++; else INSERT++;
////					dump(db, "save", key);
//
//					commit(db, COMMIT_FREQ);
//					mi.update();
//				}
//
////				boolean all = true;
////				for (String key : mEntries.keySet())
////				{
////					all &= db.get(new _KeyValue(key)).mValue.equals(mEntries.get(key));
////					FETCH++;
////				}
////				if (!all) throw new Exception("Not all keys found");
//
//				commit(db, 100);
//				dump(db, "commit", "");
//
////				ScanResult result = db.scan(new ScanResult());
////				System.out.println(result);
//			}
//
////for (int loop = 0; loop < 10; loop++)
//{
//			try (Database db = new Database(blockDevice, DatabaseOpenOption.OPEN); MonitorInstance mi = mDatabaseMonitorWindow.attach(db))
//			{
//				db.scan(new ScanResult());
//
//				try{
//				boolean all = true;
//				for (String key : mEntries.keySet())
//				{
//					all &= db.get(new _KeyValue(key)) != null;
//					FETCH++;
//				}
//				if (!all) throw new Exception("Not all keys found");
//
//				Collections.shuffle(list, RND);
//
//				for (int i = list.size()/2; --i >= 0;)
//				{
//					String key = list.get(i);
////					System.out.println(key);
//					if(mEntries.remove(key)==null) continue;
//					if(!db.remove(new _KeyValue(key))) throw new IllegalStateException("Failed to remove: " + key);
//					dump(db, "remove", key);
//					DELETE++;
//
//					commit(db, COMMIT_FREQ);
//					mi.update();
//				}
//				commit(db, 100);
//				dump(db, "commit", "");
//				}catch(Exception e){
//					dump(db, "remove exception", "?");
//					throw e;
//				}
//			}
//
//			try (Database db = new Database(blockDevice, DatabaseOpenOption.OPEN); MonitorInstance mi = mDatabaseMonitorWindow.attach(db))
//			{
//				boolean all = true;
//				for (String key : mEntries.keySet())
//				{
//					all &= db.get(new _KeyValue(key)) != null;
//					FETCH++;
//				}
//				if (!all) throw new Exception("Not all keys found");
//
//				Collections.shuffle(list, RND);
//
//				for (int i = list.size()/2; --i >= 0;)
//				{
//					String key = list.get(i);
//					String value = Helper.createString(RND);
//					mEntries.remove(key);
//					mEntries.put(key, value);
//					if (db.save(new _KeyValue(key, value))) UPDATE++; else INSERT++;
////					dump(db, "save", key);
//
//					commit(db, COMMIT_FREQ);
//					mi.update();
//				}
//
//				commit(db, 100);
//				dump(db, "commit", "");
//			}
//}
//
//			try (Database db = new Database(blockDevice, DatabaseOpenOption.OPEN); MonitorInstance mi = mDatabaseMonitorWindow.attach(db))
//			{
//				List<String> keys = new ArrayList<>(mEntries.keySet());
//				Collections.shuffle(keys, RND);
//
//				for (String key : keys)
//				{
//					if(mEntries.remove(key)==null) continue;
//					try{
//					if(!db.remove(new _KeyValue(key))) throw new IllegalStateException("Failed to remove: " + key);
//					}finally{
//					dump(db, "remove", key);
//					}
//					DELETE++;
//
//					commit(db, COMMIT_FREQ);
//					mi.update();
////					if(TESTINDEX>=54)return;
//				}
//
//				commit(db, 100);
//
//				Thread.sleep(10000_000);
//			}
//		}
//		finally
//		{
//			mStopTime = System.currentTimeMillis();
//
//			Console.println("#%d time=%s duration=%s seed=%d operations=[%d,%d,%d,%d,%d]", ++TESTROUND, Helper.formatTime(System.currentTimeMillis() - mInitTime), Helper.formatTime(mStopTime - mStartTime), seed, FETCH, INSERT, UPDATE, DELETE, COMMIT);
//		}
//	}
//
//
//	private void commit(Database aDatabase, int aProb) throws IOException
//	{
//		if (RND.nextInt(100) <= aProb)
//		{
////			if (mTreeFrame != null)
////			{
////				String description = aDatabase.scan(new ScanResult()).getDescription();
////				mTreeFrame.add(new TextSlice("" + TESTINDEX));
////				mTreeFrame.add(new TreeRenderer(new HorizontalLayout(), description));
////			}
//
////			if (aProb == 100)
//			{
//				if (mTreeFrame != null)
//				{
//					mTreeFrame.add(new TextSlice("Commit", Color.GREEN, Color.WHITE, 10));
//				}
//				aDatabase.commit();
//				COMMIT++;
//
////				ScanResult sr = aDatabase.scan(new ScanResult());
////				System.out.println(sr);
//			}
//
//			if (false)
//			{
//				long alloc = aDatabase.getBlockDevice().getAllocatedSpace() / 10;
//
//				RangeMap rangeMap = ((ManagedBlockDevice)aDatabase.getBlockDevice()).getRangeMap();
//
//				int x = 0;
//				for (int i = 0; i < alloc; i++)
//				{
//					int used = 0;
//					for (int j = 0; j < 10; j++)
//					{
//						if (rangeMap.isFree(10 * i + j, 1))
//						{
//							used++;
//							x++;
//						}
//					}
//
//					System.out.print(used <= 1 ? "-" : "█");
//				}
//				System.out.println();
//
////				System.out.println(rangeMap);
////				System.out.println(x+" "+rangeMap.getFreeSpace()+" "+rangeMap.getUsedSpace());
//
////				System.out.printf("%5d %5d %5d%n", aDatabase.getBlockDevice().getUsedSpace(), free, alloc);
//			}
//		}
//	}
//
//
//	private void dump(Database aDatabase, String aOperation, String aKey) throws IOException
//	{
//		if (mLog && mTreeFrame != null)
//		{
//			mTreeFrame.add(new TextSlice("#" + TESTINDEX + " " + aOperation + ": " + aKey));
//			mTreeFrame.add(new TreeRenderer(new HorizontalLayout(), aDatabase.scan(new ScanResult()).getDescription()));
//		}
//
//		TESTINDEX++;
//	}
}
