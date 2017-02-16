package org.terifan.raccoon.serialization;

import java.util.HashMap;


public class MarshallerRegistry
{
	private final static HashMap<EntityDescriptor, Marshaller> INSTANCES = new HashMap<>();

	
	private MarshallerRegistry()
	{
	}
	

	public static Marshaller getInstance(EntityDescriptor aTypeDeclarations)
	{
		Marshaller instance = INSTANCES.get(aTypeDeclarations);

		if (instance == null)
		{
			synchronized (MarshallerRegistry.class)
			{
				instance = INSTANCES.get(aTypeDeclarations);

				if (instance == null)
				{
					instance = new Marshaller(aTypeDeclarations);

					INSTANCES.put(aTypeDeclarations, instance);
				}
			}
		}

		return instance;
	}
}
