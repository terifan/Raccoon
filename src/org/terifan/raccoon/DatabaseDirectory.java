package org.terifan.raccoon;

import java.util.ArrayList;
import org.terifan.raccoon.blockdevice.BlockAccessor;
import org.terifan.raccoon.blockdevice.managed.ManagedBlockDevice;
import org.terifan.raccoon.document.Document;


class DatabaseDirectory
{
	private final static String DIRECTORY = "directory";

	private BTree mStorage;


	DatabaseDirectory(ManagedBlockDevice aBlockDevice)
	{
		Document conf = aBlockDevice.getMetadata().getDocument(DIRECTORY);
		if (conf == null)
		{
			conf = new Document();
		}

		mStorage = new BTree(new BlockAccessor(aBlockDevice), conf);
	}


	void commit(ManagedBlockDevice aBlockDevice)
	{
		mStorage.commit();

		aBlockDevice.getMetadata().put(DIRECTORY, mStorage.getConfiguration());
	}


	Document get(String aName)
	{
		ArrayMapEntry entry = new ArrayMapEntry(new ArrayMapKey(aName));

		if (mStorage.get(entry))
		{
			return new Document().fromByteArray(entry.getValue());
		}

		return null;
	}


	void remove(String aName)
	{
		mStorage.remove(new ArrayMapEntry(new ArrayMapKey(aName)));
	}


	void put(String aName, Document aConfiguration)
	{
		mStorage.put(new ArrayMapEntry(new ArrayMapKey(aName), aConfiguration.toByteArray(), (byte)0));
	}


	ArrayList<String> list()
	{
		ArrayList<String> list = new ArrayList<>();

		mStorage.visit(new BTreeVisitor()
		{
			@Override
			void leaf(BTree aImplementation, BTreeLeaf aNode)
			{
				aNode.mMap.forEach(entry -> list.add(entry.getKey().toString()));
			}
		});

		return list;
	}


	boolean exists(String aName)
	{
		return get(aName) != null;
	}
}
