package xperminatal;

import xperminatal.Injector.Inject;
import xperminatal.Injector.Named;


public class InjectorTest2
{
	public static void main(String ... args)
	{
		try
		{
			Injector injector = new Injector();
			injector.registerNamed(Color.class, "border", ()->new Color(255,0,0));
			injector.registerNamed(Color.class, "background", ()->new Color(0,255,0));
			injector.registerNamed(Color.class, "text", ()->new Color(0,0,255));

			ComponentByFields cls1 = injector.create(ComponentByFields.class);
			ComponentByConstructor cls2 = injector.create(ComponentByConstructor.class);
			ComponentBySetters cls3 = injector.create(ComponentBySetters.class);
			ComponentByInitializer cls4 = injector.create(ComponentByInitializer.class);

			System.out.println(cls1);
			System.out.println(cls2);
			System.out.println(cls3);
			System.out.println(cls4);
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
	}

	static class ComponentByFields
	{
		@Inject(name = "border") private Color mBorderColor;
		@Inject(name = "background") private Color mBackgroundColor;
		@Inject(name = "text") private Color mTextColor;

		public ComponentByFields()
		{
		}

		@Override
		public String toString()
		{
			return "Component{" + "mBorderColor=" + mBorderColor + ", mBackgroundColor=" + mBackgroundColor + ", mTextColor=" + mTextColor + '}';
		}
	}

	static class ComponentByConstructor
	{
		private Color mBorderColor;
		private Color mBackgroundColor;
		private Color mTextColor;

		public ComponentByConstructor()
		{
		}

		@Inject
		public ComponentByConstructor(@Named(name = "border") Color aBorderColor, @Named(name = "background") Color aBackgroundColor, @Named(name = "text") Color aTextColor)
		{
			mBorderColor = aBorderColor;
			mBackgroundColor = aBackgroundColor;
			mTextColor = aTextColor;
		}

		@Override
		public String toString()
		{
			return "Component{" + "mBorderColor=" + mBorderColor + ", mBackgroundColor=" + mBackgroundColor + ", mTextColor=" + mTextColor + '}';
		}
	}

	static class ComponentBySetters
	{
		private Color mBorderColor;
		private Color mBackgroundColor;
		private Color mTextColor;

		public ComponentBySetters()
		{
		}

		@Override
		public String toString()
		{
			return "Component{" + "mBorderColor=" + mBorderColor + ", mBackgroundColor=" + mBackgroundColor + ", mTextColor=" + mTextColor + '}';
		}

		@Inject(name = "border")
		public void setBorderColor(Color aBorderColor)
		{
			mBorderColor = aBorderColor;
		}

		@Inject(name = "background")
		public void setBackgroundColor(Color aBackgroundColor)
		{
			mBackgroundColor = aBackgroundColor;
		}

		@Inject(name = "text")
		public void setTextColor(Color aTextColor)
		{
			mTextColor = aTextColor;
		}
	}

	static class ComponentByInitializer
	{
		private Color mBorderColor;
		private Color mBackgroundColor;
		private Color mTextColor;

		public ComponentByInitializer()
		{
		}

		@Override
		public String toString()
		{
			return "Component{" + "mBorderColor=" + mBorderColor + ", mBackgroundColor=" + mBackgroundColor + ", mTextColor=" + mTextColor + '}';
		}

		@Inject
		public void initialize(@Named(name = "border") Color aBorderColor, @Named(name = "background") Color aBackgroundColor, @Named(name = "text") Color aTextColor)
		{
			mBorderColor = aBorderColor;
			mBackgroundColor = aBackgroundColor;
			mTextColor = aTextColor;
		}
	}

	static class Color
	{
		int r,g,b;

		public Color(int aR, int aG, int aB)
		{
			r = aR;
			g = aG;
			b = aB;
		}

		@Override
		public String toString()
		{
			return "Color{" + "r=" + r + ", g=" + g + ", b=" + b + '}';
		}
	}
}
