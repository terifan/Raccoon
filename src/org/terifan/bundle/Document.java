package org.terifan.bundle;

import java.io.Externalizable;
import java.io.StringReader;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;


/**
 * A Bundle is typed Map that can be serialized to JSON and binary format.
 * <p>
 * Note: the hashCode and equals methods are order independent even though the Bundle maintains elements in the inserted order.
 * </p>
 */
public class Document extends Container<String, Document> implements Externalizable, Cloneable
{
	private final static long serialVersionUID = 1L;

	LinkedHashMap<String, Object> mValues;


	public Document()
	{
		mValues = new LinkedHashMap<>();
	}


	public Document(String aJSON)
	{
		this();

		unmarshalJSON(new StringReader(aJSON));
	}


	@Override
	public Object get(String aKey)
	{
		return mValues.get(aKey);
	}


	@Override
	Document set(String aKey, Object aValue)
	{
		if (aKey == null)
		{
			throw new IllegalArgumentException("Keys cannot be null.");
		}
		mValues.put(aKey, aValue);
		return this;
	}


	public <T> T newInstance(Class<T> aType, Function<Document, T> aImporter)
	{
		return aImporter.apply(this);
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


	@Override
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


	@Override
	public boolean containsKey(String aKey)
	{
		return mValues.containsKey(aKey);
	}


	/**
	 * Return this Bundle as a compacted JSON.
	 *
	 * @return return this Bundle as a compacted JSON
	 */
	@Override
	public String toString()
	{
		return marshalJSON(new StringBuilder(), true).toString();
	}


	@Override
	Checksum hashCode(Checksum aChecksum)
	{
		mValues.entrySet().forEach(entry ->
		{
			aChecksum.update(entry.getKey());
			Object value = entry.getValue();
			super.hashCode(aChecksum, value);
		});

		return aChecksum;
	}


	@Override
	public boolean equals(Object aOther)
	{
		if (aOther instanceof Document)
		{
			return marshalJSON(true).equals(((Container)aOther).marshalJSON(true));
		}

		return false;
	}


	/**
	 * Order independent equals comparison.
	 */
	@Override
	public boolean same(Container aOther)
	{
		if (!(aOther instanceof Document))
		{
			return false;
		}
		if (aOther.size() != size())
		{
//			System.out.println("Different number of entries in provided Bundle: found: " + aOther.size() + ", expected: " + size());
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


	/**
	 * Puts all entries from the provided Bundle to this Bundle.
	 *
	 * @param aSource another bundle
	 * @return this bundle
	 */
	public Document putAll(Document aSource)
	{
		aSource.entrySet().forEach(entry -> mValues.put(entry.getKey(), entry.getValue()));
		return this;
	}


	@Override
	public Map<String, Object> toMap()
	{
		return new LinkedHashMap<>(mValues);
	}


	public void forEach(BiConsumer<? super String, ? super Object> action)
	{
		mValues.forEach(action);
	}


	public long[] getLongArray(String aKey)
	{
		Array arr = getArray(aKey);
		long[] values = new long[arr.size()];
		for (int i = 0; i < values.length; i++)
		{
			values[i] = arr.getLong(i);
		}
		return values;
	}
}