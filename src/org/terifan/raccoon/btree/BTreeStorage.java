package org.terifan.raccoon.btree;

import org.terifan.raccoon.storage.BlockAccessor;


public interface BTreeStorage extends AutoCloseable
{
	BlockAccessor getBlockAccessor();


	long getTransaction();


	@Override
	default void close()
	{
		getBlockAccessor().getBlockDevice().commit();
		getBlockAccessor().getBlockDevice().close();
	}
}
