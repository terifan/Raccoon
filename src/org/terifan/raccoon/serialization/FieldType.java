package org.terifan.raccoon.serialization;

import java.io.Serializable;


class FieldType implements Serializable
{
	private final static long serialVersionUID = 1L;

	FieldCategory category;
	String name;
	Class type;
	boolean array;
	boolean primitive;
	int depth;
	int code;
	FieldType[] componentType;


	@Override
	public String toString()
	{
		StringBuilder s = new StringBuilder();
		s.append(category);
		s.append(" ");
		s.append("("+code+")");
		s.append(" ");
		s.append(type.getSimpleName());
		for (int i = 0; i < depth; i++)
		{
			s.append("[]");
		}
		if (componentType != null)
		{
			s.append("<");
			s.append(componentType[0].type.getSimpleName());
			if (componentType[0].array)
			{
				for (int i = 0; i < componentType[0].depth; i++)
				{
					s.append("[]");
				}
			}
			if (componentType[1] != null)
			{
				s.append("," + componentType[1].type.getSimpleName());
				if (componentType[1].array)
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
