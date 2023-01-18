package test;

import org.terifan.raccoon.BTreeStorage;
import org.terifan.raccoon.BTree;
import java.util.function.Supplier;
import javax.swing.JFrame;
import org.terifan.raccoon.CompressionParam;
import org.terifan.raccoon.ScanResult;
import org.terifan.raccoon.io.managed.ManagedBlockDevice;
import org.terifan.raccoon.io.physical.IPhysicalBlockDevice;
import org.terifan.raccoon.io.physical.MemoryBlockDevice;
import org.terifan.raccoon.io.secure.AccessCredentials;
import org.terifan.raccoon.io.secure.SecureBlockDevice;
import org.terifan.raccoon.storage.BlockAccessor;
import org.terifan.treegraph.HorizontalLayout;
import org.terifan.treegraph.TreeGraph;
import org.terifan.treegraph.util.VerticalImageFrame;


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


	public static BlockAccessor createMemoryStorage()
	{
		return new BlockAccessor(new ManagedBlockDevice(new MemoryBlockDevice(512)), CompressionParam.NO_COMPRESSION);
	}


	public static BlockAccessor createSecureMemoryStorage()
	{
		return new BlockAccessor(new ManagedBlockDevice(SecureBlockDevice.open(new AccessCredentials("password"), new MemoryBlockDevice(512))), CompressionParam.NO_COMPRESSION);
	}


	public static BlockAccessor createStorage(Supplier<IPhysicalBlockDevice> aSupplier)
	{
		return new BlockAccessor(new ManagedBlockDevice(aSupplier.get()), CompressionParam.NO_COMPRESSION);
	}


	public static BlockAccessor createSecureStorage(Supplier<IPhysicalBlockDevice> aSupplier)
	{
		return new BlockAccessor(new ManagedBlockDevice(SecureBlockDevice.open(new AccessCredentials("password"), aSupplier.get())), CompressionParam.NO_COMPRESSION);
	}
}
