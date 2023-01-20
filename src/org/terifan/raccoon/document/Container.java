package org.terifan.raccoon.document;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.StringReader;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;


abstract class Container<K, R> implements Externalizable, Cloneable
{
	private final static long serialVersionUID = 1L;


	Container()
	{
	}


	public <T> T get(K aKey)
	{
		return (T)getImpl(aKey);
	}


	public <T> T get(K aKey, T aDefaultValue)
	{
		Object v = getImpl(aKey);
		if (v == null)
		{
			return aDefaultValue;
		}
		return (T)v;
	}


	public <T> T get(K aKey, Function<K, T> aDefaultValue)
	{
		Object v = getImpl(aKey);
		if (v == null)
		{
			return aDefaultValue.apply(aKey);
		}
		return (T)v;
	}


	public <T> T get(K aKey, Supplier<T> aDefaultValue)
	{
		Object v = getImpl(aKey);
		if (v == null)
		{
			return aDefaultValue.get();
		}
		return (T)v;
	}


	public Boolean getBoolean(K aKey)
	{
		Object v = getImpl(aKey);
		if (v == null)
		{
			return null;
		}
		if (v instanceof Boolean)
		{
			return (Boolean)v;
		}
		if (v instanceof String)
		{
			return Boolean.valueOf((String)v);
		}
		if (v instanceof Number)
		{
			return ((Number)v).longValue() != 0;
		}
		throw new IllegalArgumentException("Value of key " + aKey + " cannot be cast on a Boolean");
	}


	public Byte getByte(K aKey)
	{
		Object v = getImpl(aKey);
		if (v == null)
		{
			return null;
		}
		if (v instanceof Number)
		{
			return ((Number)v).byteValue();
		}
		throw new IllegalArgumentException("Value of key " + aKey + " cannot be cast on a Byte");
	}


	public Short getShort(K aKey)
	{
		Object v = getImpl(aKey);
		if (v == null)
		{
			return null;
		}
		if (v instanceof Number)
		{
			return ((Number)v).shortValue();
		}
		throw new IllegalArgumentException("Value of key " + aKey + " cannot be cast on a Short");
	}


	public Integer getInt(K aKey)
	{
		Object v = getImpl(aKey);
		if (v == null)
		{
			return null;
		}
		if (v instanceof Number)
		{
			return ((Number)v).intValue();
		}
		throw new IllegalArgumentException("Value of key " + aKey + " cannot be cast on an Integer");
	}


	public Long getLong(K aKey)
	{
		Object v = getImpl(aKey);
		if (v == null)
		{
			return null;
		}
		if (v instanceof Number)
		{
			return ((Number)v).longValue();
		}
		throw new IllegalArgumentException("Value of key " + aKey + " cannot be cast on a Long");
	}


	public Float getFloat(K aKey)
	{
		Object v = getImpl(aKey);
		if (v == null)
		{
			return null;
		}
		if (v instanceof Number)
		{
			return ((Number)v).floatValue();
		}
		throw new IllegalArgumentException("Value of key " + aKey + " (" + v.getClass().getSimpleName() + ") cannot be cast on a Double");
	}


	public Double getDouble(K aKey)
	{
		Object v = getImpl(aKey);
		if (v == null)
		{
			return null;
		}
		if (v instanceof Number)
		{
			return ((Number)v).doubleValue();
		}
		throw new IllegalArgumentException("Value of key " + aKey + " (" + v.getClass().getSimpleName() + ") cannot be cast on a Double");
	}


	public String getString(K aKey)
	{
		Object v = getImpl(aKey);
		if (v == null)
		{
			return null;
		}
		return v.toString();
	}


	public LocalDate getDate(K aKey)
	{
		Object v = getImpl(aKey);
		if (v == null)
		{
			return null;
		}
		if (v instanceof LocalDate)
		{
			return (LocalDate)v;
		}
		if (v instanceof String)
		{
			return LocalDate.parse((String)v);
		}
		throw new IllegalArgumentException("Value of key " + aKey + " (" + v.getClass().getSimpleName() + ") cannot be cast on a LocalDate");
	}


	public LocalTime getTime(K aKey)
	{
		Object v = getImpl(aKey);
		if (v == null)
		{
			return null;
		}
		if (v instanceof LocalTime)
		{
			return (LocalTime)v;
		}
		if (v instanceof String)
		{
			return LocalTime.parse((String)v);
		}
		throw new IllegalArgumentException("Value of key " + aKey + " (" + v.getClass().getSimpleName() + ") cannot be cast on a LocalTime");
	}


	public LocalDateTime getDateTime(K aKey)
	{
		Object v = getImpl(aKey);
		if (v == null)
		{
			return null;
		}
		if (v instanceof LocalDateTime)
		{
			return (LocalDateTime)v;
		}
		if (v instanceof String)
		{
			return LocalDateTime.parse((String)v);
		}
		throw new IllegalArgumentException("Value of key " + aKey + " (" + v.getClass().getSimpleName() + ") cannot be cast on a LocalDateTime");
	}


	public OffsetDateTime getOffsetDateTime(K aKey)
	{
		Object v = getImpl(aKey);
		if (v == null)
		{
			return null;
		}
		if (v instanceof OffsetDateTime)
		{
			return (OffsetDateTime)v;
		}
		if (v instanceof String)
		{
			return OffsetDateTime.parse((String)v);
		}
		throw new IllegalArgumentException("Value of key " + aKey + " (" + v.getClass().getSimpleName() + ") cannot be cast on a OffsetDateTime");
	}


	public UUID getUUID(K aKey)
	{
		Object v = getImpl(aKey);
		if (v == null)
		{
			return null;
		}
		if (v instanceof UUID)
		{
			return (UUID)v;
		}
		if (v instanceof String)
		{
			String s = (String)v;
			if (s.length() == 36 && s.matches("[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"))
			{
				return UUID.fromString(s);
			}
		}
		if (v instanceof Array)
		{
			Array a = (Array)v;
			if (a.size() == 2 && (a.getImpl(0) instanceof Long) && (a.getImpl(1) instanceof Long))
			{
				return new UUID(a.getLong(0), a.getLong(1));
			}
		}
		throw new IllegalArgumentException("Value of key " + aKey + " (" + v.getClass().getSimpleName() + ") cannot be cast on a UUID");
	}


	public Number getNumber(K aKey)
	{
		Object v = getImpl(aKey);
		if (v == null)
		{
			return null;
		}
		if (v instanceof Number)
		{
			return (Number)v;
		}
		throw new IllegalArgumentException("Value of key " + aKey + " (" + v.getClass().getSimpleName() + ") cannot be cast on a Number");
	}


	public Array getArray(K aKey)
	{
		return (Array)getImpl(aKey);
	}


	public Document getDocument(K aKey)
	{
		return (Document)getImpl(aKey);
	}


	public byte[] getBinary(K aKey)
	{
		Object v = getImpl(aKey);
		if (v == null)
		{
			return null;
		}
		if (v instanceof byte[])
		{
			return (byte[])v;
		}
		if (v instanceof String)
		{
			String s = (String)v;

			if (s.matches("[a-zA-Z0-9\\-\\=\\\\].*"))
			{
				return Base64.getDecoder().decode(s);
			}

			return s.getBytes();
		}

		throw new IllegalArgumentException("Unsupported format: " + v.getClass());
	}


	public R put(K aKey, Object aValue)
	{
		if (aValue == null
			|| aValue instanceof String
			|| aValue instanceof Number
			|| aValue instanceof Document
			|| aValue instanceof Array
			|| aValue instanceof Boolean
			|| aValue instanceof LocalDateTime
			|| aValue instanceof LocalDate
			|| aValue instanceof LocalTime
			|| aValue instanceof OffsetDateTime
			|| aValue instanceof UUID
			|| aValue instanceof byte[])
		{
			putImpl(aKey, aValue);
		}
		else
		{
			throw new IllegalArgumentException("Unsupported type: " + aValue.getClass());
		}

		return (R)this;
	}


	public boolean isNull(K aKey)
	{
		return getImpl(aKey) == null;
	}


	@Override
	public int hashCode()
	{
		return hashCode(new Checksum()).getValue();
	}


	void hashCode(Checksum aChecksum, Object aValue)
	{
		if (aValue instanceof Container)
		{
			((Container)aValue).hashCode(aChecksum);
		}
		else if (aValue instanceof CharSequence)
		{
			aChecksum.update((CharSequence)aValue);
		}
		else if (aValue instanceof byte[])
		{
			byte[] buf = (byte[])aValue;
			aChecksum.update(buf, 0, buf.length);
		}
		else
		{
			int hashCode = Objects.hashCode(aValue);
			aChecksum.update(0xFF & (hashCode >>> 24));
			aChecksum.update(0xFF & (hashCode >> 16));
			aChecksum.update(0xFF & (hashCode >> 8));
			aChecksum.update(0xFF & hashCode);
		}
	}


	public byte[] marshal()
	{
		try
		{
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			marshal(baos);
			return baos.toByteArray();
		}
		catch (IOException e)
		{
			throw new IllegalArgumentException(e);
		}
	}


	public void marshal(OutputStream aOutputStream) throws IOException
	{
		VarOutputStream out = new VarOutputStream(aOutputStream);
		out.writeObject(this);
	}


	public static <T> T unmarshal(byte[] aBinaryData)
	{
		return unmarshal(new ByteArrayInputStream(aBinaryData));
	}


	public static <T> T unmarshal(InputStream aBinaryData)
	{
		try
		{
			return (T)new VarInputStream(aBinaryData).readObject();
		}
		catch (IOException e)
		{
			throw new IllegalStateException(e);
		}
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
		try ( VarOutputStream out = new VarOutputStream(tmp))
		{
			out.writeObject(this);
		}
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

		Container tmp = (Container)new VarInputStream(in).readObject();
		clear();

		if (tmp instanceof Document)
		{
			((Document)this).putAll((Document)tmp);
		}
		else
		{
			((Array)this).addAll((Array)tmp);
		}
	}


	public String marshalJSON(boolean aCompact)
	{
		StringBuilder builder = new StringBuilder();
		marshalJSON(builder, aCompact);
		return builder.toString();
	}


	public <T extends Appendable> T marshalJSON(T aOutput, boolean aCompact)
	{
		try
		{
			new JSONEncoder().marshal(new JSONTextWriter(aOutput, aCompact), this);
		}
		catch (IOException e)
		{
			throw new IllegalArgumentException(e);
		}
		return aOutput;
	}


	public OutputStream marshalJSON(OutputStream aOutput, boolean aCompact)
	{
		try
		{
			OutputStreamWriter out = new OutputStreamWriter(aOutput);
			new JSONEncoder().marshal(new JSONTextWriter(out, aCompact), this);
			out.flush();
		}
		catch (IOException e)
		{
			throw new IllegalArgumentException(e);
		}

		return aOutput;
	}


	public static <R> R unmarshalJSON(String aJSONData)
	{
		return unmarshalJSON(new StringReader(aJSONData));
	}


	public static <R> R unmarshalJSON(Reader aJSONData)
	{
		try
		{
			return (R)new JSONDecoder().unmarshal(aJSONData);
		}
		catch (IOException e)
		{
			throw new IllegalArgumentException(e);
		}
	}


	public static boolean isSupportedType(Object aValue)
	{
		Class type = aValue == null ? null : aValue.getClass();

		return false
			|| type == Document.class
			|| type == Array.class
			|| type == String.class
			|| type == Integer.class
			|| type == Long.class
			|| type == Double.class
			|| type == Boolean.class
			|| type == Byte.class
			|| type == Short.class
			|| type == Float.class
			|| type == LocalDate.class
			|| type == LocalTime.class
			|| type == LocalDateTime.class
			|| type == OffsetDateTime.class
			|| type == UUID.class
			|| type == byte[].class
			|| type == null;
	}


	public Map<K, Object> toMap()
	{
		LinkedHashMap<K, Object> map = new LinkedHashMap<>();
		for (K key : keySet())
		{
			map.put(key, getImpl(key));
		}
		return map;
	}


	/**
	 * Performs a deep clone of this Container.
	 */
	@Override
	public R clone()
	{
		return unmarshal(marshal());
	}


	abstract Object getImpl(K aKey);


	abstract R putImpl(K aKey, Object aValue);


	public abstract boolean containsKey(K aKey);


	abstract Checksum hashCode(Checksum aChecksum);


	public abstract R remove(K aKey);


	public abstract int size();


	public abstract R clear();


	public abstract boolean same(R aOther);


	public abstract Set<K> keySet();
}
