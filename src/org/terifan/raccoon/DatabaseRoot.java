package org.terifan.raccoon;

import org.terifan.raccoon.btree.ArrayMapKey;
import org.terifan.raccoon.btree.ArrayMapEntry;
import org.terifan.raccoon.btree.BTreeVisitor;
import org.terifan.raccoon.btree.BTree;
import org.terifan.raccoon.btree.BTreeLeafNode;
import java.util.ArrayList;
import org.terifan.raccoon.blockdevice.BlockAccessor;
import org.terifan.raccoon.blockdevice.managed.ManagedBlockDevice;
import org.terifan.raccoon.btree.BTreeConfiguration;
import org.terifan.raccoon.btree.OpResult;
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


	synchronized BTreeConfiguration get(Object aValue)
	{
		OpResult op = mStorage.get(new ArrayMapKey(aValue));

		if (op.state == OpState.MATCH)
		{
			return new BTreeConfiguration(op.entry.getValue());
		}

		return null;
	}


	synchronized void remove(String aName)
	{
		mStorage.remove(new ArrayMapKey(aName));
	}


	synchronized void put(String aName, Document aConfiguration)
	{
		mStorage.put(new ArrayMapEntry(new ArrayMapKey(aName), aConfiguration, (byte)0));
	}


	synchronized ArrayList<String> list()
	{
		ArrayList<String> list = new ArrayList<>();

		mStorage.visit(new BTreeVisitor()
		{
			@Override
			public boolean leaf(BTreeLeafNode aNode)
			{
				aNode.forEachEntry(entry ->
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


	synchronized boolean exists(String aName)
	{
		return get(aName) != null;
	}
}
