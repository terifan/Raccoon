package org.terifan.raccoon;

import java.util.Map.Entry;
import java.util.TreeMap;


class NodeBuffer
{
	private TreeMap<MarshalledKey, BTreeNode> mMap;


	public NodeBuffer()
	{
		mMap = new TreeMap<>((o1, o2) -> o1.compareTo(o2));
	}


	BTreeNode get(MarshalledKey aKey)
	{
		return mMap.get(aKey);
	}


	void put(MarshalledKey aKey, BTreeNode aNode)
	{
		mMap.put(aKey, aNode);
	}


	BTreeNode remove(MarshalledKey aKey)
	{
		return mMap.remove(aKey);
	}


	void clear()
	{
		mMap.clear();
	}


	Iterable<Entry<MarshalledKey, BTreeNode>> entrySet()
	{
		return mMap.entrySet();
	}


	Iterable<MarshalledKey> keySet()
	{
		return mMap.keySet();
	}
}
