package org.terifan.raccoon;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;
import org.terifan.raccoon.util.Result;


class NodeBuffer implements Iterable<ArrayMapEntry>
{
	private TreeMap<ArrayMapKey, BTreeNode> mNodes;
	private ArrayMap mArrayMap;


	public NodeBuffer(ArrayMap aMap)
	{
		mArrayMap = aMap;
		mNodes = new TreeMap<>((o1, o2) -> o1.compareTo(o2));
	}


	BTreeNode get(ArrayMapKey aKey)
	{
		return mNodes.get(aKey);
	}


	void put(ArrayMapKey aKey, BTreeNode aNode)
	{
		mNodes.put(aKey, aNode);
	}


	BTreeNode remove(ArrayMapKey aKey)
	{
		return mNodes.remove(aKey);
	}


	void clear()
	{
		mNodes.clear();
	}


	Iterable<Entry<ArrayMapKey, BTreeNode>> entrySet()
	{
		return mNodes.entrySet();
	}


	Iterable<ArrayMapKey> keySet()
	{
		return mNodes.keySet();
	}


	Collection<BTreeNode> values()
	{
		return mNodes.values();
	}


	int getUsedSpace()
	{
		return mArrayMap.getUsedSpace();
	}


	ArrayMapEntry getEntry(int aIndex, ArrayMapEntry aEntry)
	{
		return mArrayMap.get(aIndex, aEntry);
	}


	boolean getEntry(ArrayMapEntry aEntry)
	{
		return mArrayMap.get(aEntry);
	}


	void putEntry(ArrayMapEntry aArrayMapEntry)
	{
		mArrayMap.put(aArrayMapEntry, null);
	}


	void putEntry(ArrayMapEntry aArrayMapEntry, Result<ArrayMapEntry> aObject)
	{
		mArrayMap.put(aArrayMapEntry, aObject);
	}


	void removeEntry(int aIndex, Result<ArrayMapEntry> aObject)
	{
		mArrayMap.remove(aIndex, aObject);
	}


	void removeEntry(ArrayMapKey aArrayMapEntry, Result<ArrayMapEntry> aObject)
	{
		mArrayMap.remove(aArrayMapEntry, aObject);
	}


	void insertEntry(ArrayMapEntry aArrayMapEntry)
	{
		mArrayMap.insert(aArrayMapEntry);
	}


	int getFreeSpace()
	{
		return mArrayMap.getFreeSpace();
	}


	ArrayMapKey getKey(int aIndex)
	{
		return mArrayMap.getKey(aIndex);
	}


	ArrayMapEntry getFirst()
	{
		return mArrayMap.getFirst();
	}


	ArrayMapEntry removeFirst()
	{
		return mArrayMap.removeFirst();
	}


	ArrayMapEntry getLast()
	{
		return mArrayMap.getLast();
	}


	void clearEntries()
	{
		mArrayMap.clear();
	}


	int size()
	{
		return mArrayMap.size();
	}


	byte[] array()
	{
		return mArrayMap.array();
	}


	int nearestIndex(ArrayMapKey aKey)
	{
		return mArrayMap.nearestIndex(aKey);
	}


	void loadNearestEntry(ArrayMapEntry aNearestEntry)
	{
		mArrayMap.loadNearestEntry(aNearestEntry);
	}


	ArrayMap[] split(Integer aCapacity)
	{
		return mArrayMap.split(aCapacity);
	}


	@Override
	public Iterator<ArrayMapEntry> iterator()
	{
		return mArrayMap.iterator();
	}


	@Override
	public String toString()
	{
		return mArrayMap.toString();
	}


	String integrityCheck()
	{
		return mArrayMap.integrityCheck();
	}
}
