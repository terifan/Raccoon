package org.terifan.raccoon;

import java.util.ArrayList;
import org.terifan.raccoon.BTreeNode.VisitorState;
import org.terifan.raccoon.blockdevice.BlockAccessor;
import org.terifan.raccoon.blockdevice.managed.ManagedBlockDevice;
import org.terifan.raccoon.document.Document;
import org.terifan.raccoon.document.ObjectId;


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
		Document conf = get(aName);
		mStorage.remove(new ArrayMapEntry(new ArrayMapKey(conf.getObjectId("_id"))));
		mStorage.remove(new ArrayMapEntry(new ArrayMapKey(conf.getString("name"))));
	}


	void put(Document aConfiguration)
	{
		mStorage.put(new ArrayMapEntry(new ArrayMapKey(aConfiguration.getObjectId("_id")), aConfiguration, (byte)0));
		mStorage.put(new ArrayMapEntry(new ArrayMapKey(aConfiguration.getString("name")), aConfiguration, (byte)0));
	}


	ArrayList<String> list()
	{
		ArrayList<String> list = new ArrayList<>();

		mStorage.visit(new BTreeVisitor()
		{
			@Override
			boolean leaf(BTree aImplementation, BTreeLeafNode aNode)
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
