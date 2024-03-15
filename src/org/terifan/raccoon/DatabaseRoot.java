package org.terifan.raccoon;

import org.terifan.raccoon.btree.BTreeVisitor;
import org.terifan.raccoon.btree.BTree;
import org.terifan.raccoon.btree.BTreeLeafNode;
import java.util.ArrayList;
import org.terifan.raccoon.blockdevice.BlockAccessor;
import org.terifan.raccoon.blockdevice.managed.ManagedBlockDevice;
import org.terifan.raccoon.btree.BTreeConfiguration;
import org.terifan.raccoon.btree.ArrayMapEntry;
import org.terifan.raccoon.btree.ArrayMapEntry.Type;
import org.terifan.raccoon.btree.OpState;
import org.terifan.raccoon.document.Document;


class DatabaseRoot
{
	private BTree mStorage;


	DatabaseRoot(ManagedBlockDevice aBlockDevice, BTreeConfiguration aConfiguration)
	{
		assert aConfiguration != null;

		mStorage = new BTree(new BlockAccessor(aBlockDevice), aConfiguration);
	}


	public BTreeConfiguration getConfiguration()
	{
		return mStorage.getConfiguration();
	}


	synchronized boolean commit(ManagedBlockDevice aBlockDevice)
	{
		return mStorage.commit();
	}


	synchronized BTreeConfiguration get(String aName)
	{
		ArrayMapEntry entry = new ArrayMapEntry().setKeyInstance(aName);

		mStorage.get(entry);

		if (entry.getState() == OpState.MATCH)
		{
			return new BTreeConfiguration(entry.getValueInstance());
		}

		return null;
	}


	synchronized void remove(String aName)
	{
		mStorage.remove(new ArrayMapEntry().setKeyInstance(aName));
	}


	synchronized void put(String aName, Document aConfiguration)
	{
		mStorage.put(new ArrayMapEntry().setKeyInstance(aName).setValueInstance(aConfiguration.put("name", aName)));
	}


	synchronized ArrayList<String> list()
	{
		ArrayList<String> list = new ArrayList<>();

		mStorage.visit(new BTreeVisitor()
		{
			@Override
			public boolean leaf(BTreeLeafNode aNode)
			{
				aNode.forEachEntry(entry -> list.add(entry.toKeyString()));
				return true;
			}
		});

		return list;
	}


	synchronized boolean exists(String aName)
	{
		return get(aName) != null;
	}
}
