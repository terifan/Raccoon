package org.terifan.raccoon.serialization;

import java.util.HashMap;


public class MarshallerRegistry
{
	private final static HashMap<EntityDescriptor, Marshaller> mMarshallers = new HashMap<>();

	
	private MarshallerRegistry()
	{
	}
	

	public static synchronized Marshaller getInstance(EntityDescriptor aTypeDeclarations)
	{
		Marshaller instance = mMarshallers.get(aTypeDeclarations);

		if (instance == null)
		{
			instance = new Marshaller(aTypeDeclarations);

			mMarshallers.put(aTypeDeclarations, instance);
		}

		return instance;
	}
}
