package org.terifan.raccoon.document;

import java.io.Externalizable;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;


public class Array extends Container<Integer, Array> implements Externalizable, Iterable, Cloneable
{
	private final static long serialVersionUID = 1L;

	final ArrayList<Object> mValues;


	public Array()
	{
		mValues = new ArrayList<>();
	}


	public Array(int aInitialCapacity)
	{
		mValues = new ArrayList<>(aInitialCapacity);
	}


	/**
	 * Add the item to this Array. If the value provided is an array, list or stream an Array is created.
	 */
	public Array add(Object aValue)
	{
		if (aValue != null && aValue.getClass().isArray())
		{
			Array arr = new Array();
			addImpl(arr);

			for (int i = 0, len = java.lang.reflect.Array.getLength(aValue); i < len; i++)
			{
				Object v = java.lang.reflect.Array.get(aValue, i);
				arr.add(v);
			}
		}
		else if (aValue instanceof Iterable)
		{
			Array arr = new Array();
			addImpl(arr);

			((Iterable)aValue).forEach(arr::addImpl);
		}
		else if (aValue instanceof Stream)
		{
			Array arr = new Array();
			addImpl(arr);

			((Stream)aValue).forEach(arr::addImpl);
		}
		else
		{
			addImpl(aValue);
		}

		return this;
	}


	/**
	 * Add each item provided, same as calling the <code>add</code> method for each item.
	 */
	public Array addAll(Object... aValue)
	{
		for (Object o : aValue)
		{
			add(o);
		}

		return this;
	}


	protected void addImpl(Object aValue)
	{
		mValues.add(aValue);
	}


	@Override
	public <T> T get(Integer aIndex)
	{
		return (T)mValues.get(aIndex);
	}


	@Override
	Array set(Integer aIndex, Object aValue)
	{
		if (aIndex == mValues.size())
		{
			mValues.add(aValue);
		}
		else
		{
			mValues.set(aIndex, aValue);
		}
		return this;
	}


	@Override
	public int size()
	{
		return mValues.size();
	}


	@Override
	public Array clear()
	{
		mValues.clear();
		return this;
	}


	@Override
	public Array remove(Integer aIndex)
	{
		mValues.remove((int)aIndex);
		return this;
	}


	@Override
	public boolean containsKey(Integer aKey)
	{
		return aKey != null && mValues.size() > aKey;
	}


	@Override
	public Iterator iterator()
	{
		return mValues.iterator();
	}


	public Stream stream()
	{
		return mValues.stream();
	}


	@Override
	public String toString()
	{
		return marshalJSON(true);
	}


	@Override
	Checksum hashCode(Checksum aChecksum)
	{
		mValues.forEach(value -> super.hashCode(aChecksum, value));

		return aChecksum;
	}


	@Override
	public boolean equals(Object aOther)
	{
		if (aOther instanceof Array)
		{
			return mValues.equals(((Array)aOther).mValues);
		}

		return false;
	}


	/**
	 * Order independent equals comparison.
	 */
	@Override
	public boolean same(Container aOther)
	{
		if (!(aOther instanceof Array))
		{
			return false;
		}
		if (aOther.size() != mValues.size())
		{
//			System.out.println("Different number of entries in provided Array: found: " + aOther.size() + ", expected: " + size());
			return false;
		}

		for (int i = 0; i < mValues.size(); i++)
		{
			Object value = get(i);
			Object otherValue = aOther.get(i);

			if ((value instanceof Container) && (otherValue instanceof Container))
			{
				if (!((Container)value).same((Container)otherValue))
				{
					return false;
				}
			}
			else if (!value.equals(otherValue))
			{
				return false;
			}
		}

		return true;
	}


	/**
	 * Create an array of item provided including primitives and arrays.
	 *
	 * Note: if the object provided is an array it will be consumed, e.g. these two samples will result in the same structure (json: "[1,2,3,4]"):
	 * <code>
	 *    Array.of(1,2,3,4);
	 *
	 *    int[] values = {1,2,3,4};
	 *    Array.of(values);
	 * </code>
	 *
	 * @param aValue an array of objects
	 * @return an array
	 */
	public static Array of(Object aValue)
	{
		return ofImpl(aValue);
	}


	public static Array ofImpl(Object aValue)
	{
		Array array = new Array();

		if (aValue != null && aValue.getClass().isArray())
		{
			for (int i = 0, len = java.lang.reflect.Array.getLength(aValue); i < len; i++)
			{
				Object v = java.lang.reflect.Array.get(aValue, i);

				if (v != null && v.getClass().isArray())
				{
					array.addImpl(ofImpl(v));
				}
				else
				{
					array.addImpl(v);
				}
			}
		}
		else if (aValue instanceof Iterable)
		{
			((Iterable)aValue).forEach(array::add);
		}
		else if (aValue instanceof Stream)
		{
			((Stream)aValue).forEach(array::add);
		}
		else if (aValue == null || isSupportedType(aValue))
		{
			array.add(aValue);
		}
		else
		{
			throw new IllegalArgumentException("Unsupported type: " + aValue.getClass());
		}

		return array;
	}


	/**
	 * Create an array of item provided including primitives and arrays.
	 *
	 * @param aValues an array of objects
	 * @return an array
	 */
	public static Array of(Object... aValues)
	{
		return ofImpl(aValues);
	}


	@Override
	public Map<Integer, Object> toMap()
	{
		LinkedHashMap<Integer, Object> map = new LinkedHashMap<>();
		int i = 0;
		for (Object v : mValues)
		{
			map.put(i++, v);
		}
		return map;
	}


	@Override
	public Set<Integer> keySet()
	{
		return new AbstractSet<Integer>()
		{
			@Override
			public Iterator<Integer> iterator()
			{
				return new Iterator<Integer>()
				{
					int index;

					@Override
					public boolean hasNext()
					{
						return index < mValues.size();
					}


					@Override
					public Integer next()
					{
						if (index >= mValues.size())
						{
							throw new IllegalStateException();
						}
						return index++;
					}
				};
			}


			@Override
			public int size()
			{
				return mValues.size();
			}
		};
	}


	/**
	 * Returns an Iterable over the items in the Array with each item cast to the provided type.
	 */
	public <T> Iterable<T> iterable(Class<T> aType)
	{
		return () -> new Iterator<T>()
		{
			int mIndex;

			@Override
			public boolean hasNext()
			{
				return mIndex < mValues.size();
			}


			@Override
			public T next()
			{
				return (T)mValues.get(mIndex++);
			}
		};
	}


	public <T> Stream<T> stream(Class<T> aType)
	{
		return (Stream<T>)mValues.stream();
	}


	public <T> void visit(Consumer<T> aConsumer)
	{
		mValues.forEach(e -> aConsumer.accept((T)e));
	}
}
