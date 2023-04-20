package org.terifan.raccoon;

import java.util.Collection;
import java.util.Map.Entry;
import java.util.TreeMap;


class NodeBuffer
{
	private TreeMap<ArrayMapKey, BTreeNode> mMap;


	public NodeBuffer()
	{
		mMap = new TreeMap<>((o1, o2) -> o1.compareTo(o2));
	}


	BTreeNode get(ArrayMapKey aKey)
	{
		return mMap.get(aKey);
	}


	void put(ArrayMapKey aKey, BTreeNode aNode)
	{
		mMap.put(aKey, aNode);
	}


	BTreeNode remove(ArrayMapKey aKey)
	{
		return mMap.remove(aKey);
	}


	void clear()
	{
		mMap.clear();
	}


	Iterable<Entry<ArrayMapKey, BTreeNode>> entrySet()
	{
		return mMap.entrySet();
	}


	Iterable<ArrayMapKey> keySet()
	{
		return mMap.keySet();
	}


	Collection<BTreeNode> values()
	{
		return mMap.values();
	}
}
