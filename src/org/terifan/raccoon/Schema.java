package org.terifan.raccoon;

import java.util.ArrayList;
import java.util.Iterator;



public class Schema
{
	private String mName;
	private Table mTable;


	Schema(Table aTable)
	{
		mTable = aTable;

		mName = mTable.getName();
	}


	public String getName()
	{
		return mName;
	}


	public Iterator<Entry> iteratorRaw()
	{
		return mTable.iteratorRaw();
	}


	@Override
	public String toString()
	{
		return mName;
	}


	public Iterable<String[]> getFields()
	{
		return new ArrayList<>();
	}
}
