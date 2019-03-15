package xperminatal;

import java.lang.reflect.Constructor;
import java.util.HashMap;


public class Factory
{
	private HashMap<Class,Class> mMappings;
	private HashMap<Class,Producer> mProducers;


	public Factory()
	{
		mMappings = new HashMap<>();
		mProducers = new HashMap<>();
	}


	public void register(Class aFrom, Class aTo)
	{
		mMappings.put(aFrom, aTo);
	}


	public void register(Class aFrom, Producer aProducer)
	{
		mProducers.put(aFrom, aProducer);
	}


	public <T> T getInstance(Class<T> aType, Object... aParameters)
	{
		try
		{
			aType = mMappings.getOrDefault(aType, aType);

			Producer producer = mProducers.get(aType);
			if (producer != null)
			{
				return (T)producer.create(aParameters);
			}

			T instance = null;

			Class[] types = new Class[aParameters.length];
			for (int i = 0; i < aParameters.length; i++)
			{
				types[i] = aParameters[i].getClass();
			}

			for (Constructor constructor : aType.getConstructors())
			{
				Class[] parameterTypes = constructor.getParameterTypes();
				if (parameterTypes.length == types.length)
				{
					boolean ok = true;
					for (int i = 0; i < parameterTypes.length; i++)
					{
						if (!parameterTypes[i].isAssignableFrom(types[i]))
						{
							ok = false;
							break;
						}
					}

					if (ok)
					{
//						Object[] values = createMappedValues(constructor.getParameterTypes());
//						instance = (T)constructor.newInstance(values);
						instance = (T)constructor.newInstance(aParameters);
						break;
					}
				}
			}

//			if (instance == null)
//			{
//				throw new IllegalArgumentException("No matching constructor found for type " + aType);
//			}

			if (instance == null)
			{
				instance = (T)aType.newInstance();
			}

//			prepareInstance(aType, instance);

			return instance;
		}
		catch (Exception e)
		{
			throw new IllegalStateException(e);
		}
	}


//	private <T> void prepareInstance(Class<T> aType, T aInstance) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException, SecurityException
//	{
//		for (Field field : aType.getDeclaredFields())
//		{
//			if (field.getAnnotation(Inject.class) != null)
//			{
//				field.setAccessible(true);
//				field.set(aInstance, create(mMappings.get(field.getType())));
//			}
//		}
//		for (Method method : aType.getDeclaredMethods())
//		{
//			if (method.getAnnotation(Inject.class) != null)
//			{
//				method.setAccessible(true);
//				method.invoke(aInstance, createMappedValues(method.getParameterTypes()));
//			}
//		}
//	}


	private Object[] createMappedValues(Class[] paramTypes)
	{
		Object[] values = new Object[paramTypes.length];
		for (int i = 0; i < paramTypes.length; i++)
		{
			Class type = mMappings.get(paramTypes[i]);
			values[i] = getInstance(type);
		}
		return values;
	}


	interface Producer
	{
		Object create(Object[] aParameters);
	}
}
