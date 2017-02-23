package org.terifan.raccoon.serialization;

import java.util.HashMap;


public class MarshallerFactory
{
	private final static HashMap<EntityDescriptor, Marshaller> INSTANCES = new HashMap<>();


	private MarshallerFactory()
	{
	}


	public static synchronized Marshaller getInstance(EntityDescriptor aTypeDeclarations)
	{
		Marshaller instance = INSTANCES.get(aTypeDeclarations);

		if (instance == null)
		{
			instance = new Marshaller(aTypeDeclarations);
			INSTANCES.put(aTypeDeclarations, instance);
		}

		return instance;
	}
}
