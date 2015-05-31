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


	@Override
	public void writeExternal(ObjectOutput aOut) throws IOException
	{
		aOut.writeUTF(category.name());
		aOut.writeUTF(name);
		aOut.writeUTF(type.getName());
		aOut.writeBoolean(nullable);
		aOut.write(depth);
		aOut.writeUTF(format.name());
		aOut.writeBoolean(componentType == null);
		if (componentType != null)
		{
			aOut.write(componentType.length);
			for (FieldType fieldType : componentType)
			{
				fieldType.writeExternal(aOut);
			}
		}
	}


	@Override
	public void readExternal(ObjectInput aIn) throws IOException, ClassNotFoundException
	{
		category = FieldCategory.valueOf(aIn.readUTF());
		name = aIn.readUTF();
		nullable = aIn.readBoolean();
		depth = aIn.read();
		format = FieldFormat.valueOf(aIn.readUTF());
		if (aIn.readBoolean())
		{
			componentType = null;
		}
		else
		{
			componentType = new FieldType[aIn.read()];
			for (int i = 0; i < componentType.length; i++)
			{
				componentType[i].readExternal(aIn);
			}
		}
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
