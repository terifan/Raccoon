package xperminatal;


public class InjectorTest
{
	public static void main(String ... args)
	{
		try
		{
			Injector injector = new Injector();
			injector.register(Red.class, DarkRed.class);
			injector.register(Color.class, Red.class);

			ColorBucket cls = injector.create(ColorBucket.class);

			System.out.println(cls.getColor());

			injector.register(Color.class, Blue.class);

			ColorBucket cls2 = injector.create(ColorBucket.class);

			System.out.println(cls2.getColor());
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
	}

	static class ColorBucket
	{
		//@Inject
		Color mColor;

		public ColorBucket()
		{
		}

		//@Inject
		public ColorBucket(Color aColor)
		{
			mColor = aColor;
		}

		public Color getColor()
		{
			return mColor;
		}

		public void setColor(Color aColor)
		{
			mColor = aColor;
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
