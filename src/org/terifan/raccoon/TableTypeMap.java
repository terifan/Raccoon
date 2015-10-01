package org.terifan.raccoon;

import java.util.ArrayList;
import java.util.Arrays;


class TableTypeMap
{
	private final ArrayList<TableType> mTableTypes;


	public TableTypeMap()
	{
		mTableTypes = new ArrayList<>();
	}


	void add(TableType aTableType)
	{
		mTableTypes.add(aTableType);
	}


	TableType get(Class aType, Object aDiscriminator)
	{
		for (TableType tableType : mTableTypes)
		{
			if (tableType.getType() == aType)
			{
				if (aDiscriminator == null)
				{
					return tableType;
				}

				byte[] discriminator = tableType.createDiscriminatorKey(aDiscriminator);

				if (Arrays.equals(discriminator, tableType.getDiscriminatorKey()))
				{
					return tableType;
				}
			}
		}

		return null;
	}
}
