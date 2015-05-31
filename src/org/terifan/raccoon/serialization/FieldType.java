package org.terifan.raccoon.serialization;

import java.io.Serializable;


class FieldType implements Serializable, Comparable<FieldType>
{
	private final static long serialVersionUID = 1L;

	FieldCategory category;
	String name;
	Class type;
	boolean primitive;
	int depth;
	FieldFormat format;
	FieldType[] componentType;


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
		return s.toString();
	}
}
