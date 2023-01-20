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
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.BiConsumer;


public class Document extends Container<String, Document> implements Externalizable, Cloneable
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
	public String toString()
	{
		return "Document{size=" + mValues.size() + "}";
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


	public Document fromJson(String aJson)
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


	public Document fromByteArray(byte[] aBinaryData)
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
}
