package org.terifan.raccoon.btree;

import org.terifan.bundle.Document;
import org.terifan.raccoon.io.managed.IManagedBlockDevice;
import org.terifan.raccoon.storage.BlockAccessor;


public interface BTreeStorage extends AutoCloseable
{
	BlockAccessor getBlockAccessor();


	long getTransaction();


	default IManagedBlockDevice getBlockDevice()
	{
		return getBlockAccessor().getBlockDevice();
	}


	@Override
	default void close()
	{
		getBlockDevice().commit();
		getBlockDevice().close();
	}


	default Document getApplicationHeader()
	{
		return getBlockAccessor().getBlockDevice().getApplicationHeader();
	}
}
