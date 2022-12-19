package org.terifan.raccoon;

import org.terifan.bundle.Document;
import org.terifan.raccoon.io.managed.IManagedBlockDevice;
import org.terifan.raccoon.storage.BlockAccessor;


public interface BTreeStorage extends AutoCloseable
{
	BlockAccessor getBlockAccessor();


	long getTransaction();


	@Override
	default void close()
	{
		getBlockDevice().commit();
		getBlockDevice().close();
	}


	default IManagedBlockDevice getBlockDevice()
	{
		return getBlockAccessor().getBlockDevice();
	}


	default Document getApplicationMetadata()
	{
		return getBlockAccessor().getBlockDevice().getApplicationMetadata();
	}
}
