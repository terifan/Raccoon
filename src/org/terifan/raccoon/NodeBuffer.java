package org.terifan.raccoon;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;
import org.terifan.raccoon.util.Result;


class NodeBuffer implements Iterable<ArrayMapEntry>
{
	private TreeMap<ArrayMapKey, BTreeNode> mNodes;
	private ArrayMap mMap;


	public NodeBuffer(ArrayMap aMap)
	{
		mMap = aMap;
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
		return mMap.getUsedSpace();
	}


	ArrayMapEntry getEntry(int aIndex, ArrayMapEntry aEntry)
	{
		return mMap.get(aIndex, aEntry);
	}


	boolean getEntry(ArrayMapEntry aEntry)
	{
		return mMap.get(aEntry);
	}


	void putEntry(ArrayMapEntry aArrayMapEntry)
	{
		mMap.put(aArrayMapEntry, null);
	}


	void putEntry(ArrayMapEntry aArrayMapEntry, Result<ArrayMapEntry> aObject)
	{
		mMap.put(aArrayMapEntry, aObject);
	}


	void removeEntry(int aIndex, Result<ArrayMapEntry> aObject)
	{
		mMap.remove(aIndex, aObject);
	}


	void removeEntry(ArrayMapKey aArrayMapEntry, Result<ArrayMapEntry> aObject)
	{
		mMap.remove(aArrayMapEntry, aObject);
	}


	void insertEntry(ArrayMapEntry aArrayMapEntry)
	{
		mMap.insert(aArrayMapEntry);
	}


	int getFreeSpace()
	{
		return mMap.getFreeSpace();
	}


	ArrayMapKey getKey(int aIndex)
	{
		return mMap.getKey(aIndex);
	}


	ArrayMapEntry getFirst()
	{
		return mMap.getFirst();
	}


	ArrayMapEntry removeFirst()
	{
		return mMap.removeFirst();
	}


	ArrayMapEntry getLast()
	{
		return mMap.getLast();
	}


	void clearEntries()
	{
		mMap.clear();
	}


	int size()
	{
		return mMap.size();
	}


	byte[] array()
	{
		return mMap.array();
	}


	int nearestIndex(ArrayMapKey aKey)
	{
		return mMap.nearestIndex(aKey);
	}


	void loadNearestEntry(ArrayMapEntry aNearestEntry)
	{
		mMap.loadNearestEntry(aNearestEntry);
	}


	ArrayMap[] split(Integer aCapacity)
	{
		return mMap.split(aCapacity);
	}


	@Override
	public Iterator<ArrayMapEntry> iterator()
	{
		return mMap.iterator();
	}


	@Override
	public String toString()
	{
		return mMap.toString();
	}


	String integrityCheck()
	{
		return mMap.integrityCheck();
	}
}
