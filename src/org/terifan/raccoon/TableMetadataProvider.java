package org.terifan.raccoon;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.locks.ReentrantReadWriteLock;


final class TableMetadataProvider
{
	private final ArrayList<TableMetadata> mTableMetadatas;
	private final ReentrantReadWriteLock mReadWriteLock = new ReentrantReadWriteLock();


	TableMetadataProvider()
	{
		mTableMetadatas = new ArrayList<>();
	}


	TableMetadata getOrCreate(Class aType, Object aDiscriminator)
	{
		mReadWriteLock.readLock().lock();

		try
		{
			for (TableMetadata tableMetadata : mTableMetadatas)
			{
				if (tableMetadata.getType() == aType)
				{
					if (aDiscriminator == null)
					{
						return tableMetadata;
					}

					byte[] discriminator = tableMetadata.createDiscriminatorKey(aDiscriminator); // TODO: use same discriminator on all tables?

					if (Arrays.equals(discriminator, tableMetadata.getDiscriminatorKey()))
					{
						return tableMetadata;
					}
				}
			}
		}
		finally
		{
			mReadWriteLock.readLock().unlock();
		}

		try
		{
			mReadWriteLock.writeLock().lock();

			TableMetadata tableMetadata = new TableMetadata(aType, aDiscriminator);
			mTableMetadatas.add(tableMetadata);

			return tableMetadata;
		}
		finally
		{
			mReadWriteLock.writeLock().unlock();
		}
	}
}
