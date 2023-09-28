package test_racoon;

import java.io.IOException;
import java.util.Random;
import org.terifan.raccoon.BTree;
import java.util.function.Supplier;
import javax.swing.JFrame;
import org.terifan.raccoon.ScanResult;
import org.terifan.raccoon.blockdevice.BlockAccessor;
import org.terifan.raccoon.blockdevice.managed.ManagedBlockDevice;
import org.terifan.raccoon.blockdevice.physical.MemoryBlockDevice;
import org.terifan.raccoon.blockdevice.secure.AccessCredentials;
import org.terifan.raccoon.blockdevice.secure.SecureBlockDevice;
import org.terifan.treegraph.HorizontalLayout;
import org.terifan.treegraph.TreeGraph;
import org.terifan.treegraph.util.VerticalImageFrame;
import org.terifan.raccoon.blockdevice.physical.PhysicalBlockDevice;


public class _Tools
{
	private static VerticalImageFrame mFrame;
	static TreeGraph mGraph;


	public static void showTree(BTree aTree)
	{
		if (mFrame == null)
		{
			mFrame = new VerticalImageFrame();
			mFrame.getFrame().setExtendedState(JFrame.MAXIMIZED_BOTH);
		}
		if (mGraph != null)
		{
			mFrame.remove(mGraph);
		}
		mGraph = new TreeGraph(new HorizontalLayout(), aTree.scan(new ScanResult()).getDescription());
		mFrame.add(mGraph);
	}


	public static BlockAccessor createMemoryStorage() throws IOException
	{
		return new BlockAccessor(new ManagedBlockDevice(new MemoryBlockDevice(512)), true);
	}


	public static BlockAccessor createSecureMemoryStorage() throws IOException
	{
		return new BlockAccessor(new ManagedBlockDevice(new SecureBlockDevice(new AccessCredentials("password"), new MemoryBlockDevice(512))), true);
	}


	public static BlockAccessor createStorage(Supplier<PhysicalBlockDevice> aSupplier) throws IOException
	{
		return new BlockAccessor(new ManagedBlockDevice(aSupplier.get()), true);
	}


	public static BlockAccessor createSecureStorage(Supplier<PhysicalBlockDevice> aSupplier) throws IOException
	{
		return new BlockAccessor(new ManagedBlockDevice(new SecureBlockDevice(new AccessCredentials("password"), aSupplier.get())), true);
	}


	static String createString(Random rnd)
	{
		return createString(rnd, 75 + rnd.nextInt(50));
	}


	static String createString(Random rnd, int aLength)
	{
		String alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < aLength; i++)
		{
			sb.append(alphabet.charAt(rnd.nextInt(alphabet.length())));
		}
		return sb.toString();
	}


	static byte[] createBinary(Random rnd)
	{
		byte[] bytes = new byte[5 + rnd.nextInt(40)];
		rnd.nextBytes(bytes);
		return bytes;
	}


	static String formatTime(long aMillis)
	{
		return String.format("%d:%02d.%03d", aMillis/60000, (aMillis/1000)%60, aMillis % 1000);
	}
}
