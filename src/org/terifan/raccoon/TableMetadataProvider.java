package org.terifan.raccoon;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.locks.ReentrantReadWriteLock;


final class TableMetadataProvider
{
	private final ArrayList<TableMetadata> mTableMetadatas;
	private final ReentrantReadWriteLock mReadWriteLock = new ReentrantReadWriteLock();
	private final ReentrantReadWriteLock.ReadLock mReadLock = mReadWriteLock.readLock();
	private final ReentrantReadWriteLock.WriteLock mWriteLock = mReadWriteLock.writeLock();


	TableMetadataProvider()
	{
		mTableMetadatas = new ArrayList<>();
	}


	TableMetadata getOrCreate(Class aType, Object aDiscriminator)
	{
		mReadLock.lock();

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
			mReadLock.unlock();
		}

		try
		{
			mWriteLock.lock();

			// TODO: uggly!!
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
			
			TableMetadata tableMetadata = new TableMetadata(aType, aDiscriminator);
			mTableMetadatas.add(tableMetadata);

			return tableMetadata;
		}
		finally
		{
			mWriteLock.unlock();
		}
	}
}
