package org.terifan.raccoon;

import java.util.ArrayList;
import org.terifan.raccoon.blockdevice.BlockAccessor;
import org.terifan.raccoon.blockdevice.managed.ManagedBlockDevice;
import org.terifan.raccoon.document.Document;


class DatabaseRoot
{
	private BTree mStorage;


	DatabaseRoot(ManagedBlockDevice aBlockDevice, Document aConfiguration)
	{
		mStorage = new BTree(new BlockAccessor(aBlockDevice), aConfiguration);
	}


	Document commit(ManagedBlockDevice aBlockDevice)
	{
		mStorage.commit();
		return mStorage.getConfiguration();
	}


	Document get(Object aValue)
	{
		ArrayMapEntry entry = new ArrayMapEntry(new ArrayMapKey(aValue));

		if (mStorage.get(entry))
		{
			return entry.getValue();
		}

		return null;
	}


	void remove(String aName)
	{
		mStorage.remove(new ArrayMapEntry(new ArrayMapKey(aName)));
	}


	void put(String aName, Document aConfiguration)
	{
		mStorage.put(new ArrayMapEntry(new ArrayMapKey(aName), aConfiguration, (byte)0));
	}


	ArrayList<String> list()
	{
		ArrayList<String> list = new ArrayList<>();

		mStorage.visit(new BTreeVisitor()
		{
			@Override
			boolean leaf(BTreeLeafNode aNode)
			{
				aNode.mMap.forEach(entry ->
				{
					if (entry.getKey().get() instanceof String)
					{
						list.add(entry.getKey().toString());
					}
				});
				return true;
			}
		});

		return list;
	}


	boolean exists(String aName)
	{
		return get(aName) != null;
	}
}
