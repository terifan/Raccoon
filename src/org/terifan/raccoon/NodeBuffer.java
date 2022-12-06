package org.terifan.raccoon;

import java.util.Map.Entry;
import java.util.TreeMap;


class NodeBuffer
{
	private TreeMap<MarshalledKey, BTreeNode> mBuffer;


	public NodeBuffer()
	{
		mBuffer = new TreeMap<>((o1, o2) -> o1.compareTo(o2));
	}


	Iterable<Entry<MarshalledKey, BTreeNode>> entrySet()
	{
		return mBuffer.entrySet();
	}


	void clear()
	{
		mBuffer.clear();
	}


	BTreeNode get(MarshalledKey aKey)
	{
		return mBuffer.get(aKey);
	}


	BTreeNode remove(MarshalledKey aKey)
	{
		return mBuffer.remove(aKey);
	}


	void put(MarshalledKey aKey, BTreeNode aNode)
	{
		assert aNode != null;

		mBuffer.put(aKey, aNode);
	}


	Iterable<MarshalledKey> keySet()
	{
		return mBuffer.keySet();
	}
}
