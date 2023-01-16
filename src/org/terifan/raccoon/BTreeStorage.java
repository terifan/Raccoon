package org.terifan.raccoon;

import org.terifan.bundle.Document;
import org.terifan.raccoon.io.managed.IManagedBlockDevice;
import org.terifan.raccoon.storage.BlockAccessor;


public abstract class BTreeStorage implements AutoCloseable
{
	protected abstract BlockAccessor getBlockAccessor();


	protected abstract long getTransaction();


	@Override
	public void close()
	{
		getBlockDevice().commit();
		getBlockDevice().close();
	}


	protected IManagedBlockDevice getBlockDevice()
	{
		return getBlockAccessor().getBlockDevice();
	}


	protected Document getApplicationMetadata()
	{
		return getBlockAccessor().getBlockDevice().getApplicationMetadata();
	}
}
