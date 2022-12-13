package test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;
import org.terifan.raccoon.LobOpenOption;
import org.terifan.raccoon.CompressionParam;
import org.terifan.raccoon.RaccoonDatabase;
import org.terifan.raccoon.LogLevel;
import org.terifan.raccoon.DatabaseOpenOption;
import org.terifan.raccoon.io.physical.MemoryBlockDevice;
import org.terifan.raccoon.util.Log;
import org.terifan.raccoon.LobByteChannel;


public class TestBlobs
{
	public static void main(String ... args)
	{
		try
		{
////			Log.setLevel(LogLevel.DEBUG);
//
//			MemoryBlockDevice blockDevice = new MemoryBlockDevice(4096);
//
//			byte[] writeAll = new byte[10 * 1024 * 1024];
////			byte[] writeAll = new byte[10000];
//			new Random().nextBytes(writeAll);
//
//			try (Database db = new Database(blockDevice, DatabaseOpenOption.CREATE_NEW, CompressionParam.NO_COMPRESSION))
//			{
//				_BlobKey1K key = new _BlobKey1K("test");
//
//				try (LobByteChannel blob = db.openLob(key, LobOpenOption.REPLACE))
//				{
//					blob.write(ByteBuffer.wrap(writeAll));
//				}
//
//				db.commit();
//			}
//
//			byte[] readAll;
//
//			try (Database db = new Database(blockDevice, DatabaseOpenOption.OPEN, CompressionParam.NO_COMPRESSION))
//			{
//				_BlobKey1K key = new _BlobKey1K("test");
//
//				try (LobByteChannel blob = db.openLob(key, LobOpenOption.READ))
//				{
//					readAll = new byte[(int)blob.size()];
//					blob.read(ByteBuffer.wrap(readAll));
//				}
//			}
//
////			System.out.println("---------");
////			Log.hexDump(writeAll);
////			System.out.println("---------");
////			Log.hexDump(readAll);
////			System.out.println("---------");
//
//			System.out.println(Arrays.equals(writeAll, readAll));
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
	}
}
