package xperminatal;

import java.lang.annotation.Annotation;
import static java.lang.annotation.ElementType.CONSTRUCTOR;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import java.lang.annotation.Retention;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import java.lang.annotation.Target;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.function.Supplier;


public class Injector
{
	private HashMap<Class, Class> mMappings;
	private HashMap<Class, HashMap<String, Supplier<Object>>> mNamedMappings;


	public Injector()
	{
		mMappings = new HashMap<>();
		mNamedMappings = new HashMap<>();
	}


	public void register(Class aFrom, Class aTo)
	{
		mMappings.put(aFrom, aTo);
	}


	public void registerNamed(Class aFrom, String aName, Supplier<Object> aTo)
	{
		mNamedMappings.computeIfAbsent(aFrom, e -> new HashMap<>()).put(aName, aTo);
	}


	public <T> T create(Class<T> aType)
	{
		if (aType == null)
		{
			throw new IllegalArgumentException("Provided argument is null.");
		}

		try
		{
			Class newType = mMappings.getOrDefault(aType, aType);

			if (newType == null)
			{
				throw new IllegalArgumentException(aType + " not registered");
			}

			T instance = null;

			for (Constructor constructor : newType.getConstructors())
			{
				Inject annotation = (Inject)constructor.getAnnotation(Inject.class);

				if (annotation != null)
				{
					Object[] values = createMappedValues(annotation, constructor.getParameterTypes(), constructor.getParameterAnnotations());
					instance = (T)constructor.newInstance(values);
					break;
				}
			}

			if (instance == null)
			{
				instance = (T)newType.newInstance();
			}

			prepareInstance(newType, instance);

			return instance;
		}
		catch (SecurityException | InstantiationException | IllegalAccessException | InvocationTargetException e)
		{
			throw new IllegalStateException(e);
		}
	}


	private <T> void prepareInstance(Class<T> aType, T aInstance) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException, SecurityException
	{
		for (Field field : aType.getDeclaredFields())
		{
			if (field.getAnnotation(Inject.class) != null)
			{
				field.setAccessible(true);

				Inject annotation = field.getAnnotation(Inject.class);

				if (!annotation.name().isEmpty())
				{
					Object mappedType = mNamedMappings.get(field.getType()).get(annotation.name()).get();

					if (mappedType instanceof Class)
					{
						mappedType = create((Class)mappedType);
					}

					field.set(aInstance, mappedType);
				}
				else
				{
					Class mappedType = mMappings.get(field.getType());

					if (mappedType == null)
					{
						throw new IllegalArgumentException("Type not mapped: " + field.getType());
					}

					field.set(aInstance, create(mappedType));
				}
			}
		}
		for (Method method : aType.getDeclaredMethods())
		{
			Inject annotation = method.getAnnotation(Inject.class);

			if (annotation != null)
			{
				method.setAccessible(true);
				method.invoke(aInstance, createMappedValues(annotation, method.getParameterTypes(), method.getParameterAnnotations()));
			}
		}
	}


	private Object[] createMappedValues(Inject aInjectAnnotation, Class[] paramTypes, Annotation[][] aAnnotations)
	{
		Object[] values = new Object[paramTypes.length];
		for (int i = 0; i < paramTypes.length; i++)
		{
			Class paramType = paramTypes[i];

			String name = aInjectAnnotation.name();

			for (Annotation ann : aAnnotations[i])
			{
				if (ann instanceof Named)
				{
					name = ((Named)ann).name();
				}
			}

			if (!name.isEmpty())
			{
				Object mappedType = mNamedMappings.get(paramType).get(name).get();

				if (mappedType instanceof Class)
				{
					values[i] = create((Class)mappedType);
				}
				else
				{
					values[i] = mappedType;
				}
			}
			else
			{
				Class mappedType = mMappings.get(paramType);

				if (mappedType == null)
				{
					throw new IllegalArgumentException("Type not mapped: " + paramType);
				}

				values[i] = create(mappedType);
			}
		}
		return values;
	}


	@Retention(RUNTIME)
	@Target(
		{
			METHOD, FIELD, CONSTRUCTOR
		})
	public @interface Inject
	{
		String name() default "";
	}


	@Retention(RUNTIME)
	@Target(
		{
			PARAMETER
		})
	public @interface Named
	{
		String name();
	}
}
