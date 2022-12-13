package org.terifan.raccoon.btree;

import java.util.function.Supplier;
import javax.swing.JFrame;
import org.terifan.raccoon.CompressionParam;
import org.terifan.raccoon.ScanResult;
import org.terifan.raccoon.io.IBlockDevice;
import org.terifan.raccoon.io.managed.ManagedBlockDevice;
import org.terifan.raccoon.io.physical.IPhysicalBlockDevice;
import org.terifan.raccoon.io.physical.MemoryBlockDevice;
import org.terifan.raccoon.io.secure.AccessCredentials;
import org.terifan.raccoon.io.secure.SecureBlockDevice;
import org.terifan.raccoon.storage.BlockAccessor;
import org.terifan.treegraph.HorizontalLayout;
import org.terifan.treegraph.TreeRenderer;
import org.terifan.treegraph.util.VerticalImageFrame;


public class _Tools
{
	public static void showTree(BTree aTree)
	{
		VerticalImageFrame frame = new VerticalImageFrame();
		frame.getFrame().setExtendedState(JFrame.MAXIMIZED_BOTH);
		frame.add(new TreeRenderer(new HorizontalLayout(), aTree.scan(new ScanResult()).getDescription()));
	}


	public static BTreeStorage createMemoryStorage()
	{
		return createStorage(new MemoryBlockDevice(512));
	}


	public static BTreeStorage createSecureMemoryStorage()
	{
		return createSecureStorage(new MemoryBlockDevice(512));
	}


	public static BTreeStorage createStorage(Supplier<IPhysicalBlockDevice> aSupplier)
	{
		return createStorage(aSupplier.get());
	}


	public static BTreeStorage createSecureStorage(Supplier<IPhysicalBlockDevice> aSupplier)
	{
		return createSecureStorage(aSupplier.get());
	}


	public static BTreeStorage createStorage(IPhysicalBlockDevice aPhysicalBlockDevice)
	{
		BTreeStorage storage = new BTreeStorage()
		{
			BlockAccessor blockAccessor = new BlockAccessor(new ManagedBlockDevice(aPhysicalBlockDevice), CompressionParam.NO_COMPRESSION);


			@Override
			public BlockAccessor getBlockAccessor()
			{
				return blockAccessor;
			}


			@Override
			public long getTransaction()
			{
				return 0;
			}
		};
		return storage;
	}


	public static BTreeStorage createSecureStorage(IPhysicalBlockDevice aPhysicalBlockDevice)
	{
		BTreeStorage storage = new BTreeStorage()
		{
			AccessCredentials ac = new AccessCredentials("password");
			SecureBlockDevice sec = aPhysicalBlockDevice.length() > 0 ? SecureBlockDevice.open(ac, aPhysicalBlockDevice) : SecureBlockDevice.create(ac, aPhysicalBlockDevice);
			BlockAccessor blockAccessor = new BlockAccessor(new ManagedBlockDevice(sec), CompressionParam.NO_COMPRESSION);


			@Override
			public BlockAccessor getBlockAccessor()
			{
				return blockAccessor;
			}


			@Override
			public long getTransaction()
			{
				return 0;
			}
		};
		return storage;
	}
}
