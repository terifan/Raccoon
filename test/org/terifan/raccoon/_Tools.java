package org.terifan.raccoon;

import java.io.IOException;
import java.util.function.Supplier;
import javax.swing.JFrame;
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
		return new BlockAccessor(new ManagedBlockDevice(new MemoryBlockDevice(512)), false);
	}


	public static BlockAccessor createSecureMemoryStorage() throws IOException
	{
		return new BlockAccessor(new ManagedBlockDevice(SecureBlockDevice.create(new AccessCredentials("password"), new MemoryBlockDevice(512))), false);
	}


	public static BlockAccessor createStorage(Supplier<PhysicalBlockDevice> aSupplier) throws IOException
	{
		PhysicalBlockDevice device = aSupplier.get();
		if (device.size() > 0)
		{
			return new BlockAccessor(new ManagedBlockDevice(SecureBlockDevice.open(new AccessCredentials("password"), device)), false);
		}
		return new BlockAccessor(new ManagedBlockDevice(device), false);
	}


	public static BlockAccessor createSecureStorage(Supplier<PhysicalBlockDevice> aSupplier) throws IOException
	{
		PhysicalBlockDevice device = aSupplier.get();
		if (device.size() > 0)
		{
			return new BlockAccessor(new ManagedBlockDevice(SecureBlockDevice.open(new AccessCredentials("password"), device)), false);
		}
		return new BlockAccessor(new ManagedBlockDevice(SecureBlockDevice.create(new AccessCredentials("password"), device)), false);
	}


	public static BlockAccessor createSecureStorage(PhysicalBlockDevice aDevice) throws IOException
	{
		if (aDevice.size() > 0)
		{
			return new BlockAccessor(new ManagedBlockDevice(SecureBlockDevice.open(new AccessCredentials("password"), aDevice)), false);
		}
		return new BlockAccessor(new ManagedBlockDevice(SecureBlockDevice.create(new AccessCredentials("password"), aDevice)), false);
	}
}
