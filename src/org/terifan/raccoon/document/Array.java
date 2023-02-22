package org.terifan.raccoon.document;

import java.io.Externalizable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.stream.Stream;


public class Array extends KeyValueCollection<Integer, Array> implements Iterable, Externalizable, Cloneable, Comparable<Array>
{
	private final static long serialVersionUID = 1L;

	private final ArrayList<Object> mValues;


	public Array()
	{
		mValues = new ArrayList<>();
	}


	public Array addAll(Array aSource)
	{
		mValues.addAll(aSource.mValues);
		return this;
	}


	public <T extends Array> T put(Integer aKey, Object aValue)
	{
		if (isSupportedType(aValue))
		{
			return (T)putImpl(aKey, aValue);
		}

		throw new IllegalArgumentException("Unsupported type: " + aValue.getClass());
	}


	@Override
	Object getImpl(Integer aIndex)
	{
		return mValues.get(aIndex);
	}


	/**
	 * Add the item to this Array. If the value provided is an array, list or stream an Array is created.
	 *
	 * @return this Array
	 */
	public Array add(Object aValue)
	{
		if (aValue == null || isSupportedType(aValue))
		{
			mValues.add(aValue);
		}
		else if (aValue.getClass().isArray())
		{
			Array arr = new Array();
			for (int i = 0, len = java.lang.reflect.Array.getLength(aValue); i < len; i++)
			{
				arr.add(java.lang.reflect.Array.get(aValue, i));
			}
			mValues.add(arr);
		}
		else if (aValue instanceof Iterable)
		{
			Array arr = new Array();
			((Iterable)aValue).forEach(mValues::add);
			mValues.add(arr);
		}
		else if (aValue instanceof Stream)
		{
			Array arr = new Array();
			((Stream)aValue).forEach(mValues::add);
			mValues.add(arr);
		}
		else
		{
			throw new IllegalArgumentException("Unsupported type: " + aValue.getClass());
		}

		return this;
	}


	/**
	 * Add each item provided, same as calling the <code>add</code> method for each item.
	 *
	 * @return this Array
	 */
	public Array addAll(Object... aValue)
	{
		for (Object o : aValue)
		{
			add(o);
		}

		return this;
	}


	@Override
	Array putImpl(Integer aIndex, Object aValue)
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
	public Iterator iterator()
	{
		return mValues.iterator();
	}


	public Stream stream()
	{
		return mValues.stream();
	}


	@Override
	public Iterable<Integer> keySet()
	{
		return new Iterable<Integer>()
		{
			int i;

			@Override
			public Iterator<Integer> iterator()
			{
				return new Iterator<Integer>()
				{
					@Override
					public boolean hasNext()
					{
						return i < size();
					}


					@Override
					public Integer next()
					{
						return i++;
					}
				};
			}
		};
	}


	@Override
	Checksum hashCode(Checksum aChecksum)
	{
		aChecksum.updateInt(93090393 ^ size()); // == "array".hashCode()

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
	public boolean same(Array aOther)
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
			Object value = getImpl(i);
			Object otherValue = aOther.getImpl(i);

			if ((value instanceof KeyValueCollection) && (otherValue instanceof KeyValueCollection))
			{
				if (!((KeyValueCollection)value).same((KeyValueCollection)otherValue))
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
	 * @param aValues an array of objects
	 * @return an array
	 */
	public static Array of(Object... aValues)
	{
		Array array = new Array();

		for (Object value : aValues)
		{
			if (value == null || isSupportedType(value))
			{
				array.add(value);
			}
			else if (value.getClass().isArray())
			{
				for (int i = 0, len = java.lang.reflect.Array.getLength(value); i < len; i++)
				{
					Object v = java.lang.reflect.Array.get(value, i);

					if (v == null || !v.getClass().isArray())
					{
						array.mValues.add(v);
					}
					else
					{
						array.mValues.add(of(v));
					}
				}
			}
			else if (value instanceof Iterable)
			{
				((Iterable)value).forEach(array::add);
			}
			else if (value instanceof Stream)
			{
				((Stream)value).forEach(array::add);
			}
			else
			{
				throw new IllegalArgumentException("Unsupported type: " + value.getClass());
			}
		}

		return array;
	}


	public <T> Stream<T> stream(Class<T> aType)
	{
		return (Stream<T>)mValues.stream();
	}


	public Object[] values()
	{
		return mValues.toArray();
	}


	/**
	 * Performs a deep clone of this Array and all it's values.
	 */
	@Override
	public Array clone()
	{
		return new Array().fromByteArray(toByteArray());
	}


	@Override
	public int compareTo(Array aOther)
	{
		for (int i = 0, sz = Math.min(size(), aOther.size()); i < sz; i++)
		{
			int v = ((Comparable)get(i)).compareTo(aOther.get(i));
			if (v != 0)
			{
				return v;
			}
		}

		if (size() < aOther.size())
		{
			return -1;
		}
		if (size() > aOther.size())
		{
			return 1;
		}

		return 0;
	}


	public <T> Iterable<T> iterator(Class<T> aClass)
	{
		return new Iterable<T>()
		{
			int i;

			@Override
			public Iterator<T> iterator()
			{
				return new Iterator<T>()
				{
					@Override
					public boolean hasNext()
					{
						return i < size();
					}


					@Override
					public T next()
					{
						return get(i++);
					}
				};
			}
		};
	}
}
