package org.terifan.raccoon.document;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.stream.Stream;


public class Array extends Container<Integer, Array> implements Iterable, Externalizable, Cloneable, Comparable<Array>
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
			addImpl(aValue);
		}
		else if (aValue.getClass().isArray())
		{
			Array arr = new Array();
			for (int i = 0, len = java.lang.reflect.Array.getLength(aValue); i < len; i++)
			{
				arr.add(java.lang.reflect.Array.get(aValue, i));
			}
			addImpl(arr);
		}
		else if (aValue instanceof Iterable)
		{
			Array arr = new Array();
			((Iterable)aValue).forEach(arr::addImpl);
			addImpl(arr);
		}
		else if (aValue instanceof Stream)
		{
			Array arr = new Array();
			((Stream)aValue).forEach(arr::addImpl);
			addImpl(arr);
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


	protected void addImpl(Object aValue)
	{
		mValues.add(aValue);
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
	public String toString()
	{
		return toJson();
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
//	public static Array of(Object aValue)
//	{
//		return ofImpl(aValue);
//	}
//
//
//	public static Array ofImpl(Object aValue)
//	{
//		Array array = new Array();
//
//		if (aValue == null || isSupportedType(aValue))
//		{
//			array.add(aValue);
//		}
//		else if (aValue.getClass().isArray())
//		{
//			for (int i = 0, len = java.lang.reflect.Array.getLength(aValue); i < len; i++)
//			{
//				Object v = java.lang.reflect.Array.get(aValue, i);
//
//				if (v == null || !v.getClass().isArray())
//				{
//					array.addImpl(v);
//				}
//				else
//				{
//					array.addImpl(ofImpl(v));
//				}
//			}
//		}
//		else if (aValue instanceof Iterable)
//		{
//			((Iterable)aValue).forEach(array::add);
//		}
//		else if (aValue instanceof Stream)
//		{
//			((Stream)aValue).forEach(array::add);
//		}
//		else
//		{
//			throw new IllegalArgumentException("Unsupported type: " + aValue.getClass());
//		}
//
//		return array;
//	}


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
						array.addImpl(v);
					}
					else
					{
						array.addImpl(of(v));
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
	 * Performs a deep clone of this Container.
	 */
	@Override
	public Array clone()
	{
		return new Array().fromByteArray(toByteArray());
	}


	public String toJson()
	{
		try
		{
			StringBuilder builder = new StringBuilder();
			new JSONEncoder().marshal(new JSONTextWriter(builder, true), this);
			return builder.toString();
		}
		catch (IOException e)
		{
			throw new IllegalArgumentException(e);
		}
	}


	public Array fromJson(String aJson)
	{
		try
		{
			return new JSONDecoder().unmarshal(new StringReader(aJson), this);
		}
		catch (IOException e)
		{
			throw new IllegalArgumentException(e);
		}
	}


	public byte[] toByteArray()
	{
		try
		{
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			new VarOutputStream().write(baos, this);
			return baos.toByteArray();
		}
		catch (IOException e)
		{
			throw new IllegalStateException(e);
		}
	}


	public Array fromByteArray(byte[] aBinaryData)
	{
		try
		{
			new VarInputStream().read(new ByteArrayInputStream(aBinaryData), this);
		}
		catch (IOException e)
		{
			throw new IllegalStateException(e);
		}
		return this;
	}


	@Override
	public void writeExternal(ObjectOutput aOutputStream) throws IOException
	{
		OutputStream tmp = new OutputStream()
		{
			@Override
			public void write(int aByte) throws IOException
			{
				aOutputStream.write(aByte);
			}
		};
		new VarOutputStream().write(tmp, this);
	}


	@Override
	public void readExternal(ObjectInput aInputStream) throws IOException, ClassNotFoundException
	{
		InputStream in = new InputStream()
		{
			@Override
			public int read() throws IOException
			{
				return aInputStream.read();
			}
		};

		new VarInputStream().read(in, this);
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
}
