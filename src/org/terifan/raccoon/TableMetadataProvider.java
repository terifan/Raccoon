package org.terifan.raccoon;

import java.util.ArrayList;
import java.util.Arrays;


final class TableMetadataProvider
{
	private final ArrayList<Table> mTableMetadatas;


	TableMetadataProvider()
	{
		mTableMetadatas = new ArrayList<>();
	}


	Table getOrCreate(Database aDatabase, Class aType, DiscriminatorType aDiscriminator)
	{
		Table tableMetadata = getImpl(aType, aDiscriminator);
		
		if (tableMetadata != null)
		{
			return tableMetadata;
		}

		synchronized (aType)
		{
			tableMetadata = getImpl(aType, aDiscriminator);

			if (tableMetadata != null)
			{
				return tableMetadata;
			}

			tableMetadata = new Table(aDatabase, aType, aDiscriminator);

			mTableMetadatas.add(tableMetadata);

			return tableMetadata;
		}
	}


	private Table getImpl(Class aType, DiscriminatorType aDiscriminator)
	{
		for (int i = 0; i < mTableMetadatas.size(); i++)
		{
			Table tableMetadata = mTableMetadatas.get(i);

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

		return null;
	}
}
