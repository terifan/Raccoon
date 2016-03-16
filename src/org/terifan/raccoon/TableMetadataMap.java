package org.terifan.raccoon;

import java.util.ArrayList;
import java.util.Arrays;


final class TableMetadataMap
{
	private ArrayList<TableMetadata> mTableMetadatas;


	public TableMetadataMap()
	{
		mTableMetadatas = new ArrayList<>();
	}


	void add(TableMetadata aTableMetadata)
	{
		mTableMetadatas.add(aTableMetadata);
	}


	TableMetadata get(Class aType, Object aDiscriminator)
	{
		for (TableMetadata tableMetadata : mTableMetadatas)
		{
			if (tableMetadata.getType() == aType)
			{
				if (aDiscriminator == null)
				{
					return tableMetadata;
				}

				byte[] discriminator = tableMetadata.createDiscriminatorKey(aDiscriminator);

				if (Arrays.equals(discriminator, tableMetadata.getDiscriminatorKey()))
				{
					return tableMetadata;
				}
			}
		}

		return null;
	}
}
