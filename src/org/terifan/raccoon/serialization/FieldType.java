package org.terifan.raccoon.serialization;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


class FieldType implements Externalizable, Comparable<FieldType>
{
	private final static long serialVersionUID = 1L;

	FieldCategory category;
	String name;
	Class type;
	boolean nullable;
	int depth;
	FieldFormat format;
	FieldType[] componentType;


	public FieldType()
	{
	}


	@Override
	public void writeExternal(ObjectOutput aOut) throws IOException
	{
		aOut.writeObject(category);
		aOut.writeObject(componentType);
		aOut.writeObject(depth);
		aOut.writeObject(format);
		aOut.writeObject(name);
		aOut.writeObject(nullable);
		aOut.writeObject(type);
	}


	@Override
	public void readExternal(ObjectInput aIn) throws IOException, ClassNotFoundException
	{
		category = (FieldCategory)aIn.readObject();
		componentType = (FieldType[])aIn.readObject();
		depth = (Integer)aIn.readObject();
		format = (FieldFormat)aIn.readObject();
		name = (String)aIn.readObject();
		nullable = (Boolean)aIn.readObject();
		type = (Class)aIn.readObject();
	}


	@Override
	public int compareTo(FieldType aOther)
	{
		return name.compareTo(aOther.name);
	}


	@Override
	public String toString()
	{
		StringBuilder s = new StringBuilder();
		s.append(type.getSimpleName());
		for (int i = 0; i < depth; i++)
		{
			s.append("[]");
		}
		if (componentType != null)
		{
			s.append("<");
			s.append(componentType[0].type.getSimpleName());
			if (componentType[0].format == FieldFormat.ARRAY)
			{
				for (int i = 0; i < componentType[0].depth; i++)
				{
					s.append("[]");
				}
			}
			if (componentType[1] != null)
			{
				s.append("," + componentType[1].type.getSimpleName());
				if (componentType[1].format == FieldFormat.ARRAY)
				{
					for (int i = 0; i < componentType[1].depth; i++)
					{
						s.append("[]");
					}
				}
			}
			s.append(">");
		}
		s.append(" " + name);
		if (nullable)
		{
			s.append(" (nullable)");
		}
		return s.toString();
	}
}
