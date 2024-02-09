package org.terifan.raccoon.btree;

import org.terifan.raccoon.btree.ArrayMap;
import org.terifan.raccoon.btree.ArrayMapKey;
import org.terifan.raccoon.btree.ArrayMapEntry;
import org.terifan.raccoon.btree.BTreeNode;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;


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


	BTreeNode removeXX(ArrayMapKey aKey)
	{
		return mNodes.remove(aKey);
	}


	void remove(ArrayMapKey aKey)
	{
		mNodes.remove(aKey);
		mArrayMap.remove(aKey, null);
	}


	void remove(int aIndex)
	{
		ArrayMapEntry temp = new ArrayMapEntry();
		getEntry(aIndex, temp);
		mNodes.remove(temp.getKey());
		mArrayMap.remove(aIndex, null);
	}


	void removeEntry(int aIndex)
	{
		mArrayMap.remove(aIndex, null);
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


	void insertEntry(ArrayMapEntry aArrayMapEntry)
	{
		mArrayMap.insert(aArrayMapEntry);
	}


	void insertEntry(ArrayMapEntry aArrayMapEntry, BTreeNode aChildNode)
	{
		mArrayMap.insert(aArrayMapEntry);
		mNodes.put(aArrayMapEntry.getKey(), aChildNode);
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


	BTreeNode removeFirst()
	{
		Entry<ArrayMapKey, BTreeNode> tmp = mNodes.firstEntry();
		mArrayMap.removeFirst();
		mNodes.remove(tmp.getKey());
		return tmp.getValue();
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
