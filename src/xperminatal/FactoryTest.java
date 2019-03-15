package xperminatal;


public class FactoryTest
{
	public static void main(String ... args)
	{
		try
		{
			Factory factory = new Factory();
			factory.register(Red.class, DarkRed.class);
			factory.register(Color.class, Red.class);
//			factory.register(Color.class, o->new DarkRed());

			ColorBucket cls1 = new ColorBucket(factory);

			System.out.println(cls1.getColor());

			factory.register(Color.class, Blue.class);

			ColorBucket cls2 = new ColorBucket(factory);

			System.out.println(cls2.getColor());
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
	}

	static class ColorBucket
	{
		Color mColor;

		public ColorBucket(Factory aFactory)
		{
			mColor = aFactory.getInstance(Color.class);
		}

		public Color getColor()
		{
			return mColor;
		}
	}

	interface Color
	{
	}

	static class Red implements Color
	{
	}

	static class DarkRed extends Red
	{
	}

	static class Blue implements Color
	{
	}
}
