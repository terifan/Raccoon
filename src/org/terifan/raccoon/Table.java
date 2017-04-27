package org.terifan.raccoon;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.function.Consumer;
import org.terifan.raccoon.serialization.FieldDescriptor;
import org.terifan.raccoon.serialization.Marshaller;
import org.terifan.raccoon.serialization.EntityDescriptor;
import org.terifan.raccoon.serialization.FieldTypeCategorizer;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;


public final class Table<T>
{
	protected final static int FIELD_CATEGORY_KEY = 1;
	protected final static int FIELD_CATEGORY_DISCRIMINATOR = 2;
	protected final static int FIELD_CATEGORY_VALUE = 4;

	@Key private String mTypeName;
	@Key private byte[] mDiscriminatorKey;
	private byte[] mPointer;
	private EntityDescriptor mEntityDescriptor;

	private transient Class mType;
	private transient Marshaller mMarshaller;
	private transient Database mDatabase;
	private transient volatile int mReadLocked;


	Table()
	{
	}


	Table(Database aDatabase, Class aClass, DiscriminatorType aDiscriminator)
	{
		mType = aClass;
		mTypeName = mType.getName();
		mEntityDescriptor = new EntityDescriptor(mType, mCategorizer);
		mMarshaller = new Marshaller(mEntityDescriptor);

		mDiscriminatorKey = createDiscriminatorKey(aDiscriminator);

		if (mEntityDescriptor.getFields(FIELD_CATEGORY_KEY).isEmpty())
		{
			throw new IllegalArgumentException("Entity has no keys: " + aClass);
		}
	}


	synchronized Table initialize(Database aDatabase)
	{
		mDatabase = aDatabase;
		mMarshaller = new Marshaller(mEntityDescriptor);

		try
		{
			mType = Class.forName(mTypeName);
		}
		catch (Exception | Error e)
		{
			Log.w("Warning: entity class not accessible: %s", e.toString());
		}

		if (mType != null)
		{
			mEntityDescriptor.bind(mType);
		}

		return this;
	}


	public FieldDescriptor[] getFields()
	{
		return mEntityDescriptor.getFields();
	}


	public ArrayList<FieldDescriptor> getKeyFields()
	{
		return mEntityDescriptor.getFields(FIELD_CATEGORY_KEY);
	}


	public ArrayList<FieldDescriptor> getDiscriminatorFields()
	{
		return mEntityDescriptor.getFields(FIELD_CATEGORY_DISCRIMINATOR);
	}


	public ArrayList<FieldDescriptor> getValueFields()
	{
		return mEntityDescriptor.getFields(FIELD_CATEGORY_VALUE);
	}


	byte[] createDiscriminatorKey(DiscriminatorType aDiscriminator)
	{
		if (aDiscriminator != null && aDiscriminator.getInstance() != null)
		{
			return mMarshaller.marshal(new ByteArrayBuffer(16), aDiscriminator.getInstance(), Table.FIELD_CATEGORY_DISCRIMINATOR).trim().array();
		}

		return new byte[0];
	}


	Marshaller getMarshaller()
	{
		return mMarshaller;
	}


	public Class getType()
	{
		return mType;
	}


	public String getTypeName()
	{
		return mTypeName;
	}


	byte[] getDiscriminatorKey()
	{
		return mDiscriminatorKey;
	}


	byte[] getPointer()
	{
		return mPointer;
	}


	void setPointer(byte[] aPointer)
	{
		mPointer = aPointer;
	}


	@Override
	public boolean equals(Object aOther)
	{
		if (aOther instanceof Table)
		{
			Table other = (Table)aOther;

			return mTypeName.equals(other.mTypeName) && Arrays.equals(mDiscriminatorKey, other.mDiscriminatorKey);
		}

		return false;
	}


	@Override
	public int hashCode()
	{
		return mTypeName.hashCode() ^ Arrays.hashCode(mDiscriminatorKey);
	}


	@Override
	public String toString()
	{
		String s = getDiscriminatorDescription();

		if (s == null)
		{
			return mTypeName;
		}

		return mTypeName + "[" + s + "]";
	}


	public String getDiscriminatorDescription()
	{
		if (mDiscriminatorKey.length == 0)
		{
			return null;
		}

		ResultSet resultSet = new ResultSet(mEntityDescriptor).unmarshal(new ByteArrayBuffer(mDiscriminatorKey), FIELD_CATEGORY_DISCRIMINATOR);

		StringBuilder result = new StringBuilder();

		for (FieldDescriptor fieldType : mEntityDescriptor.getFields(FIELD_CATEGORY_DISCRIMINATOR))
		{
			if (result.length() > 0)
			{
				result.append(", ");
			}

			result.append(fieldType.getName()).append("=").append(resultSet.get(fieldType));
		}

		return result.toString();
	}


	/**
	 * Return an entity as a Java class declaration.
	 */
	public String getJavaDeclaration()
	{
		StringBuilder sb = new StringBuilder();
		sb.append("package " + mEntityDescriptor.getName().substring(0, mEntityDescriptor.getName().lastIndexOf('.')) + ";\n\n");
		sb.append("class " + mEntityDescriptor.getName().substring(mEntityDescriptor.getName().lastIndexOf('.') + 1) + "\n{\n");

		for (FieldDescriptor fieldType : mEntityDescriptor.getFields(FIELD_CATEGORY_KEY))
		{
			sb.append("\t" + "@Key " + fieldType + ";\n");
		}
		for (FieldDescriptor fieldType : mEntityDescriptor.getFields(FIELD_CATEGORY_DISCRIMINATOR))
		{
			sb.append("\t" + "@Discriminator " + fieldType + ";\n");
		}
		for (FieldDescriptor fieldType : mEntityDescriptor.getFields(FIELD_CATEGORY_VALUE))
		{
			sb.append("\t" + "" + fieldType + ";\n");
		}

		sb.append("}");

		return sb.toString();
	}


	private transient FieldTypeCategorizer mCategorizer = aField ->
	{
		if (aField.getAnnotation(Key.class) != null)
		{
			return FIELD_CATEGORY_KEY;
		}
		if (aField.getAnnotation(Discriminator.class) != null)
		{
			return FIELD_CATEGORY_DISCRIMINATOR;
		}
		return FIELD_CATEGORY_VALUE;
	};


	protected EntityDescriptor getEntityDescriptor()
	{
		return mEntityDescriptor;
	}


	public static String getCategoryName(int aCategory)
	{
		if ((aCategory & FIELD_CATEGORY_KEY) != 0)
		{
			return "Key";
		}
		if ((aCategory & FIELD_CATEGORY_DISCRIMINATOR) != 0)
		{
			return "Discriminator";
		}
		return "Value";
	}


	public int size()
	{
		return getTableType().size();
	}


	private TableInstance getTableType()
	{
		return mDatabase.openTable(this, OpenOption.OPEN);
	}


	public void forEach(Consumer<T> aConsumer)
	{
		aquireReadLock();

		try
		{
			for (Iterator<T> it = new EntityIterator(getTableType(), getTableType().getLeafIterator()); it.hasNext();)
			{
				aConsumer.accept(it.next());
			}
		}
		finally
		{
			releaseReadLock();
		}
	}


	public interface ResultSetConsumer
	{
		void handle(ResultSet aResultSet);
	}


	public void forEachResultSet(ResultSetConsumer aConsumer)
	{
		aquireReadLock();

		try
		{
			ResultSet resultSet = new ResultSet(getTableType(), getTableType().getLeafIterator());

			while (resultSet.next())
			{
				aConsumer.handle(resultSet);
			}
		}
		finally
		{
			releaseReadLock();
		}
	}


	private synchronized void releaseReadLock()
	{
		mDatabase.getReadLock().unlock();
		mReadLocked--;
	}


	private synchronized void aquireReadLock()
	{
		mDatabase.getReadLock().lock();
		mReadLocked++;
	}


	synchronized boolean isReadLocked()
	{
		return mReadLocked > 0;
	}
}
