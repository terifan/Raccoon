package org.terifan.raccoon.btree;

import org.terifan.raccoon.CompressionParam;
import org.terifan.raccoon.io.IBlockDevice;
import org.terifan.raccoon.io.managed.ManagedBlockDevice;
import org.terifan.raccoon.io.physical.IPhysicalBlockDevice;
import org.terifan.raccoon.io.physical.MemoryBlockDevice;
import org.terifan.raccoon.io.secure.AccessCredentials;
import org.terifan.raccoon.io.secure.SecureBlockDevice;
import org.terifan.raccoon.storage.BlockAccessor;


public class _Tools
{
	public static BTreeStorage createMemoryStorage()
	{
		return createStorage(new MemoryBlockDevice(512));
	}


	public static BTreeStorage createSecureMemoryStorage()
	{
		return createSecureStorage(new MemoryBlockDevice(512));
	}


	public static BTreeStorage createStorage(IPhysicalBlockDevice aPhysicalBlockDevice)
	{
		BTreeStorage storage = new BTreeStorage()
		{
			BlockAccessor blockAccessor = new BlockAccessor(new ManagedBlockDevice(aPhysicalBlockDevice), CompressionParam.BEST_COMPRESSION);


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
			BlockAccessor blockAccessor = new BlockAccessor(new ManagedBlockDevice(SecureBlockDevice.create(ac, aPhysicalBlockDevice)), CompressionParam.BEST_COMPRESSION);


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
