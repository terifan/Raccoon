package org.terifan.raccoon.document;

import java.io.Externalizable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.BiConsumer;


public final class Document extends Container<String, Document> implements Externalizable, Cloneable, Comparable<Document>
{
	private final static long serialVersionUID = 1L;

	private final LinkedHashMap<String, Object> mValues;


	public Document()
	{
		mValues = new LinkedHashMap<>();
	}


	@Override
	Object getImpl(String aKey)
	{
		return mValues.get(aKey);
	}


	@Override
	Document putImpl(String aKey, Object aValue)
	{
		if (aKey == null)
		{
			throw new IllegalArgumentException("Keys cannot be null.");
		}
		mValues.put(aKey, aValue);
		return this;
	}


	public Document putAll(Document aSource)
	{
		aSource.entrySet().forEach(entry -> mValues.put(entry.getKey(), entry.getValue()));
		return this;
	}


	@Override
	public Document remove(String aKey)
	{
		mValues.remove(aKey);
		return this;
	}


	@Override
	public Document clear()
	{
		mValues.clear();
		return this;
	}


	@Override
	public int size()
	{
		return mValues.size();
	}


	public Set<String> keySet()
	{
		return mValues.keySet();
	}


	public Set<Entry<String, Object>> entrySet()
	{
		return mValues.entrySet();
	}


	public Collection<Object> values()
	{
		return mValues.values();
	}


	public boolean containsKey(String aKey)
	{
		return mValues.containsKey(aKey);
	}


	@Override
	Checksum hashCode(Checksum aChecksum)
	{
		aChecksum.updateInt(861720859 ^ size()); // == "document".hashCode()

		mValues.entrySet().forEach(entry ->
		{
			aChecksum.updateChars(entry.getKey());
			super.hashCode(aChecksum, entry.getValue());
		});

		return aChecksum;
	}


	@Override
	public boolean equals(Object aOther)
	{
		if (aOther instanceof Document)
		{
			return toJson().equals(((Document)aOther).toJson());
		}

		return false;
	}


	/**
	 * Order independent equals comparison.
	 */
	@Override
	public boolean same(Document aOther)
	{
		if (!(aOther instanceof Document))
		{
			return false;
		}
		if (aOther.size() != mValues.size())
		{
//			System.out.println("Different number of entries: found: " + aOther.size() + ", expected: " + size());
			return false;
		}

		HashSet<String> otherKeys = new HashSet<>(aOther.keySet());

		for (String key : keySet())
		{
			Object value = get(key);
			Object otherValue = aOther.get(key);

			if ((value instanceof Container) && (otherValue instanceof Container))
			{
				if (!((Container)value).same((Container)otherValue))
				{
//					System.out.println("Value of key '" + key + "' missmatch: found: " + otherValue + ", expected: " + value);
					return false;
				}
			}
			else if (!value.equals(otherValue))
			{
//				System.out.println("Value of key '" + key + "' missmatch: found: " + otherValue + ", expected: " + value);
				return false;
			}
			otherKeys.remove(key);
		}

		return true;
	}


	public void forEach(BiConsumer<? super String, ? super Object> action)
	{
		mValues.forEach(action);
	}


	/**
	 * Performs a deep clone of this Document.
	 */
	@Override
	public Document clone()
	{
		return new Document().fromByteArray(toByteArray());
	}


	@Override
	public int compareTo(Document aOther)
	{
		ArrayList<String> thisKeys = new ArrayList<>(keySet());
		ArrayList<String> otherKeys = new ArrayList<>(aOther.keySet());

		for (String key : thisKeys.toArray(new String[0]))
		{
			Object a = get(key);
			Object b = aOther.get(key);
			thisKeys.remove(key);
			otherKeys.remove(key);

			int v = b == null ? 1 : ((Comparable)a).compareTo(b);

			if (v != 0)
			{
				return v;
			}
		}

		return otherKeys.isEmpty() ? 0 : -1;
	}


	/**
	 * Decodes the JSON and return a Document, same as using the fromJson instance method.
	 */
	public static Document of(String aJSON)
	{
		return new Document().fromJson(aJSON);
	}
}
