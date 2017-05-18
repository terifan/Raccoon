package org.terifan.raccoon.btree;

import java.util.ArrayList;
import org.terifan.raccoon.Table;
import org.terifan.raccoon.core.ArrayMap;
import org.terifan.raccoon.serialization.FieldDescriptor;
import org.terifan.raccoon.serialization.FieldReader;
import org.terifan.raccoon.util.ByteArrayBuffer;


public class FieldArrayMap extends ArrayMap
{
	private Table mTable;


	public FieldArrayMap(Table aTable, int aCapacity)
	{
		super(aCapacity);
		
		mTable = aTable;
	}


	public FieldArrayMap(Table aTable, byte[] aBuffer)
	{
		super(aBuffer);
		
		mTable = aTable;
	}


	public FieldArrayMap(Table aTable, byte[] aBuffer, int aOffset, int aCapacity)
	{
		super(aBuffer, aOffset, aCapacity);
		
		mTable = aTable;
	}


	@Override
	public int indexOf(byte[] aKey)
	{
		try
		{
			ArrayList<FieldDescriptor> keys = mTable.getKeyFields();

			ByteArrayBuffer in = new ByteArrayBuffer(aKey);

			for (FieldDescriptor fd : keys)
			{
				Object value = FieldReader.readField(fd, in, true);
			}
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.err);
		}

		return super.indexOf(aKey);
	}
}
