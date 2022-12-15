package org.terifan.bundle;

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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Base64;
import java.util.Date;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;


public abstract class Container<K, R> implements Externalizable, Cloneable
{
	private final static long serialVersionUID = 1L;


	Container()
	{
	}


	public abstract <T> T get(K aKey);


	public <T> T get(K aKey, T aDefaultValue)
	{
		Object value = get(aKey);
		if (value == null)
		{
			return aDefaultValue;
		}
		return (T)value;
	}


	abstract R set(K aKey, Object aValue);


	public Boolean getBoolean(K aKey)
	{
		return getBoolean(aKey, (Boolean)null);
	}


	public Boolean getBoolean(K aKey, Boolean aDefaultValue)
	{
		Object v = get(aKey);
		if (v == null)
		{
			return aDefaultValue;
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


	public Boolean getBoolean(K aKey, Function<K, Boolean> aProvider)
	{
		Object v = get(aKey);
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
		return compute(aProvider, aKey);
	}


	private <RT> RT compute(Function<K, RT> aProvider, K aKey)
	{
		RT v = aProvider.apply(aKey);
		put(aKey, v);
		return v;
	}


	public R putBoolean(K aKey, Boolean aValue)
	{
		set(aKey, aValue);
		return (R)this;
	}


	public Byte getByte(K aKey)
	{
		return getByte(aKey, (Byte)null);
	}


	public Byte getByte(K aKey, Byte aDefaultValue)
	{
		Object v = get(aKey);
		if (v == null)
		{
			return aDefaultValue;
		}
		if (v instanceof Number)
		{
			return ((Number)v).byteValue();
		}
		throw new IllegalArgumentException("Value of key " + aKey + " cannot be cast on a Byte");
	}


	public Byte getByte(K aKey, Function<K, Byte> aProvider)
	{
		Object v = get(aKey);
		if (v instanceof Byte)
		{
			return (Byte)v;
		}
		if (v instanceof Number)
		{
			return ((Number)v).byteValue();
		}
		return compute(aProvider, aKey);
	}


	public Short getShort(K aKey)
	{
		return getShort(aKey, (Short)null);
	}


	public Short getShort(K aKey, Short aDefaultValue)
	{
		Object v = get(aKey);
		if (v == null)
		{
			return aDefaultValue;
		}
		if (v instanceof Number)
		{
			return ((Number)v).shortValue();
		}
		throw new IllegalArgumentException("Value of key " + aKey + " cannot be cast on a Short");
	}


	public Short getShort(K aKey, Function<K, Short> aProvider)
	{
		Object v = get(aKey);
		if (v instanceof Short)
		{
			return (Short)v;
		}
		if (v instanceof Number)
		{
			return ((Number)v).shortValue();
		}
		return compute(aProvider, aKey);
	}


	public Integer getInt(K aKey)
	{
		return getInt(aKey, (Integer)null);
	}


	public Integer getInt(K aKey, Integer aDefaultValue)
	{
		Object v = get(aKey);
		if (v == null)
		{
			return aDefaultValue;
		}
		if (v instanceof Number)
		{
			return ((Number)v).intValue();
		}
		throw new IllegalArgumentException("Value of key " + aKey + " cannot be cast on an Integer");
	}


	public Integer getInt(K aKey, Function<K, Integer> aProvider)
	{
		Object v = get(aKey);
		if (v instanceof Integer)
		{
			return (Integer)v;
		}
		if (v instanceof Number)
		{
			return ((Number)v).intValue();
		}
		return compute(aProvider, aKey);
	}


	public Long getLong(K aKey)
	{
		return getLong(aKey, (Long)null);
	}


	public Long getLong(K aKey, Long aDefaultValue)
	{
		Object v = get(aKey);
		if (v == null)
		{
			return aDefaultValue;
		}
		if (v instanceof Number)
		{
			return ((Number)v).longValue();
		}
		throw new IllegalArgumentException("Value of key " + aKey + " cannot be cast on a Long");
	}


	public Long getLong(K aKey, Function<K, Long> aProvider)
	{
		Object v = get(aKey);
		if (v instanceof Long)
		{
			return (Long)v;
		}
		if (v instanceof Number)
		{
			return ((Number)v).longValue();
		}
		return compute(aProvider, aKey);
	}


	public Float getFloat(K aKey)
	{
		return getFloat(aKey, (Float)null);
	}


	public Float getFloat(K aKey, Float aDefaultValue)
	{
		Object v = get(aKey);
		if (v == null)
		{
			return aDefaultValue;
		}
		if (v instanceof Number)
		{
			return ((Number)v).floatValue();
		}
		throw new IllegalArgumentException("Value of key " + aKey + " (" + v.getClass().getSimpleName() + ") cannot be cast on a Double");
	}


	public Float getFloat(K aKey, Function<K, Float> aProvider)
	{
		Object v = get(aKey);
		if (v instanceof Float)
		{
			return (Float)v;
		}
		if (v instanceof Number)
		{
			return ((Number)v).floatValue();
		}
		return compute(aProvider, aKey);
	}


	public Double getDouble(K aKey)
	{
		return getDouble(aKey, (Double)null);
	}


	public Double getDouble(K aKey, Double aDefaultValue)
	{
		Object v = get(aKey);
		if (v == null)
		{
			return aDefaultValue;
		}
		if (v instanceof Number)
		{
			return ((Number)v).doubleValue();
		}
		throw new IllegalArgumentException("Value of key " + aKey + " (" + v.getClass().getSimpleName() + ") cannot be cast on a Double");
	}


	public Double getDouble(K aKey, Function<K, Double> aProvider)
	{
		Object v = get(aKey);
		if (v instanceof Double)
		{
			return (Double)v;
		}
		if (v instanceof Number)
		{
			return ((Number)v).doubleValue();
		}
		return compute(aProvider, aKey);
	}


	public String getString(K aKey)
	{
		return (String)get(aKey);
	}


	public String getString(K aKey, String aDefaultValue)
	{
		String value = (String)get(aKey);
		if (value == null)
		{
			return aDefaultValue;
		}
		return value;
	}


	public String getString(K aKey, Function<K, String> aProvider)
	{
		Object value = get(aKey);
		if (value instanceof String)
		{
			return (String)value;
		}
		return compute(aProvider, aKey);
	}


	public R putString(K aKey, String aValue)
	{
		set(aKey, aValue);
		return (R)this;
	}


	public Date getDate(K aKey)
	{
		return getDate(aKey, (Date)null);
	}


	public Date getDate(K aKey, Date aDefaultValue)
	{
		Object value = get(aKey);
		if (value == null)
		{
			return aDefaultValue;
		}
		if (value instanceof String)
		{
			return parseDate((String)value, aDefaultValue);
		}
		return (Date)value;
	}


	public Date getDate(K aKey, Function<K, Date> aProvider)
	{
		Object value = get(aKey);
		if (value instanceof Date)
		{
			return (Date)value;
		}
		if (value instanceof String)
		{
			return parseDate((String)value, null);
		}
		return compute(aProvider, aKey);
	}


	public R putDate(K aKey, Date aValue)
	{
		set(aKey, aValue);
		return (R)this;
	}


	protected Date parseDate(String aDateString, Date aDefaultValue)
	{
		try
		{
			if (aDateString.matches("[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}"))
			{
				return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(aDateString);
			}
			if (aDateString.matches("[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{1,3}"))
			{
				return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").parse(aDateString);
			}
		}
		catch (ParseException e)
		{
			// ignore
		}

		return aDefaultValue;
	}


	public UUID getUUID(K aKey)
	{
		return getUUID(aKey, (UUID)null);
	}


	public UUID getUUID(K aKey, UUID aDefaultValue)
	{
		Object value = get(aKey);
		if (value == null)
		{
			return aDefaultValue;
		}
		if (value instanceof String)
		{
			value = parseUUID((String)value, aDefaultValue);
		}
		return (UUID)value;
	}


	public UUID getUUID(K aKey, Function<K, UUID> aProvider)
	{
		Object value = get(aKey);
		if (value instanceof UUID)
		{
			return (UUID)value;
		}
		if (value instanceof String)
		{
			value = parseUUID((String)value, null);
			if (value instanceof UUID)
			{
				return (UUID)value;
			}
		}
		return compute(aProvider, aKey);
	}


	public R putUUID(K aKey, UUID aValue)
	{
		set(aKey, aValue);
		return (R)this;
	}


	protected Object parseUUID(String aString, Object aDefaultValue)
	{
		if (aString.length() == 36 && aString.matches("[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"))
		{
			return UUID.fromString(aString);
		}
		return aDefaultValue;
	}


	public Number getNumber(K aKey)
	{
		return (Number)get(aKey);
	}


	public Number getNumber(K aKey, Number aDefautValue)
	{
		Object value = get(aKey);
		if (value == null)
		{
			return aDefautValue;
		}
		if (value instanceof Number)
		{
			return (Number)value;
		}
		throw new IllegalArgumentException("Value of key " + aKey + " cannot be cast on a Number");
	}


	public Number getNumber(K aKey, Function<K, Number> aProvider)
	{
		Object value = get(aKey);
		if (value instanceof Number)
		{
			return (Number)value;
		}
		return compute(aProvider, aKey);
	}


	public R putNumber(K aKey, Number aValue)
	{
		set(aKey, aValue);
		return (R)this;
	}


	public Array getArray(K aKey)
	{
		return (Array)get(aKey);
	}


	public Array getArray(K aKey, Function<K, Array> aProvider)
	{
		Object value = get(aKey);
		if (value instanceof Array)
		{
			return (Array)value;
		}
		return compute(aProvider, aKey);
	}


	public R putArray(K aKey, Array aValue)
	{
		set(aKey, aValue);
		return (R)this;
	}


	public Document getBundle(K aKey)
	{
		return (Document)get(aKey);
	}


	public Document getBundle(K aKey, Function<K, Document> aProvider)
	{
		Object value = get(aKey);
		if (value instanceof Document)
		{
			return (Document)value;
		}
		return compute(aProvider, aKey);
	}


	public R putBundle(K aKey, Document aValue)
	{
		set(aKey, aValue);
		return (R)this;
	}


	public byte[] getBinary(K aKey)
	{
		Object value = get(aKey);
		if (value == null)
		{
			return null;
		}
		if (value instanceof byte[])
		{
			return (byte[])value;
		}
		if (value instanceof String)
		{
			String s = (String)value;

			if (s.matches("[a-zA-Z0-9\\-\\=\\\\].*"))
			{
				return Base64.getDecoder().decode(s);
			}

			return s.getBytes();
		}

		throw new IllegalArgumentException("Unsupported format: " + value.getClass());
	}


	public byte[] getBinary(K aKey, Function<K, byte[]> aProvider)
	{
		Object value = get(aKey);
		if (value instanceof byte[])
		{
			return (byte[])value;
		}
		if (value instanceof String)
		{
			return Base64.getDecoder().decode((String)value);
		}
		return compute(aProvider, aKey);
	}


	public R putBinary(K aKey, byte[] aBytes)
	{
		set(aKey, aBytes);
		return (R)this;
	}


	public R put(K aKey, Object aValue)
	{
		if (aValue instanceof Number)
		{
			putNumber(aKey, (Number)aValue);
		}
		else if (aValue instanceof String)
		{
			putString(aKey, (String)aValue);
		}
		else if (aValue instanceof Document)
		{
			putBundle(aKey, (Document)aValue);
		}
		else if (aValue instanceof Array)
		{
			putArray(aKey, (Array)aValue);
		}
		else if (aValue instanceof Boolean)
		{
			putBoolean(aKey, (Boolean)aValue);
		}
		else if (aValue instanceof byte[])
		{
			putBinary(aKey, (byte[])aValue);
		}
		else if (aValue instanceof UUID)
		{
			putUUID(aKey, (UUID)aValue);
		}
		else if (aValue instanceof Date)
		{
			putDate(aKey, (Date)aValue);
		}
		else if (aValue != null)
		{
			throw new IllegalArgumentException("Unsupported type: " + aValue.getClass());
		}

		return (R)this;
	}


	public boolean isNull(K aKey)
	{
		return get(aKey) == null;
	}


	public abstract boolean containsKey(K aKey);


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


	abstract Checksum hashCode(Checksum aChecksum);


	public abstract R remove(K aKey);


	public abstract int size();


	public abstract R clear();


	public abstract boolean same(Container aOther);


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


	public R marshal(OutputStream aOutputStream) throws IOException
	{
		VarOutputStream out = new VarOutputStream(aOutputStream);
		out.writeObject(this);
		return (R)this;
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
			((Array)this).addAll((((Array)tmp).mValues).toArray());
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
			|| type == Date.class
			|| type == UUID.class
			|| type == null;
	}


	public abstract Map<K, Object> toMap();


	public abstract Set<K> keySet();


	/**
	 * Performs a deep clone of this Container.
	 */
	@Override
	public R clone()
	{
		return unmarshal(marshal());
	}
}
