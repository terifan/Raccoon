package org.terifan.raccoon.serialization;

import java.util.HashMap;


public class EntityDescriptorRegistry 
{
	private final static HashMap<Class, EntityDescriptor> INSTANCES = new HashMap<>();


	private EntityDescriptorRegistry()
	{
	}


	public static EntityDescriptor getInstance(Class aType)
	{
		EntityDescriptor instance = INSTANCES.get(aType);

		if (instance == null)
		{
			synchronized (EntityDescriptorRegistry.class)
			{
				instance = INSTANCES.get(aType);

				if (instance == null)
				{
					instance = new EntityDescriptor(aType);

					INSTANCES.put(aType, instance);
				}
			}
		}

		return instance;
	}
}
