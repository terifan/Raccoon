package org.terifan.raccoon;

import java.util.ArrayList;
import java.util.Arrays;


final class TableMetadataProvider
{
	private final ArrayList<TableMetadata> mTableMetadatas;


	TableMetadataProvider()
	{
		mTableMetadatas = new ArrayList<>();
	}


	TableMetadata getOrCreate(Class aType, DiscriminatorType aDiscriminator)
	{
		TableMetadata tableMetadata = getImpl(aType, aDiscriminator);
		
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

			tableMetadata = new TableMetadata(aType, aDiscriminator);

			mTableMetadatas.add(tableMetadata);

			return tableMetadata;
		}
	}


	private TableMetadata getImpl(Class aType, DiscriminatorType aDiscriminator)
	{
		for (int i = 0; i < mTableMetadatas.size(); i++)
		{
			TableMetadata tableMetadata = mTableMetadatas.get(i);

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
