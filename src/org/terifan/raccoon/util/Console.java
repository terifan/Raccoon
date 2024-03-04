package org.terifan.raccoon.util;

import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.Map.Entry;
import org.terifan.raccoon.document.Array;
import org.terifan.raccoon.document.Document;


public class Console
{
	private static SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");


	public enum Color
	{
		// Color end string, color reset
		RESET("\033[0m"),
		// Regular
		BLACK("\033[0;30m"), // BLACK
		RED("\033[0;31m"), // RED
		GREEN("\033[0;32m"), // GREEN
		YELLOW("\033[0;33m"), // YELLOW
		BLUE("\033[0;34m"), // BLUE
		MAGENTA("\033[0;35m"), // MAGENTA
		CYAN("\033[0;36m"), // CYAN
		WHITE("\033[0;37m"), // WHITE

		// Bold
		BLACK_BOLD("\033[1;30m"), // BLACK
		RED_BOLD("\033[1;31m"), // RED
		GREEN_BOLD("\033[1;32m"), // GREEN
		YELLOW_BOLD("\033[1;33m"), // YELLOW
		BLUE_BOLD("\033[1;34m"), // BLUE
		MAGENTA_BOLD("\033[1;35m"), // MAGENTA
		CYAN_BOLD("\033[1;36m"), // CYAN
		WHITE_BOLD("\033[1;37m"), // WHITE

		// Faint
		BLACK_FAINT("\033[2;30m"), // BLACK
		RED_FAINT("\033[2;31m"), // RED
		GREEN_FAINT("\033[2;32m"), // GREEN
		YELLOW_FAINT("\033[2;33m"), // YELLOW
		BLUE_FAINT("\033[2;34m"), // BLUE
		MAGENTA_FAINT("\033[2;35m"), // MAGENTA
		CYAN_FAINT("\033[2;36m"), // CYAN
		WHITE_FAINT("\033[2;37m"), // WHITE

		// Background
		BLACK_BACKGROUND("\033[40m"), // BLACK
		RED_BACKGROUND("\033[41m"), // RED
		GREEN_BACKGROUND("\033[42m"), // GREEN
		YELLOW_BACKGROUND("\033[43m"), // YELLOW
		BLUE_BACKGROUND("\033[44m"), // BLUE
		MAGENTA_BACKGROUND("\033[45m"), // MAGENTA
		CYAN_BACKGROUND("\033[46m"), // CYAN
		WHITE_BACKGROUND("\033[47m"), // WHITE

		// High Intensity
		BLACK_BRIGHT("\033[0;90m"), // BLACK
		RED_BRIGHT("\033[0;91m"), // RED
		GREEN_BRIGHT("\033[0;92m"), // GREEN
		YELLOW_BRIGHT("\033[0;93m"), // YELLOW
		BLUE_BRIGHT("\033[0;94m"), // BLUE
		MAGENTA_BRIGHT("\033[0;95m"), // MAGENTA
		CYAN_BRIGHT("\033[0;96m"), // CYAN
		WHITE_BRIGHT("\033[0;97m"), // WHITE

		// Bold High Intensity
		BLACK_BOLD_BRIGHT("\033[1;90m"), // BLACK
		RED_BOLD_BRIGHT("\033[1;91m"), // RED
		GREEN_BOLD_BRIGHT("\033[1;92m"), // GREEN
		YELLOW_BOLD_BRIGHT("\033[1;93m"), // YELLOW
		BLUE_BOLD_BRIGHT("\033[1;94m"), // BLUE
		MAGENTA_BOLD_BRIGHT("\033[1;95m"), // MAGENTA
		CYAN_BOLD_BRIGHT("\033[1;96m"), // CYAN
		WHITE_BOLD_BRIGHT("\033[1;97m"), // WHITE

		// High Intensity backgrounds
		BLACK_BACKGROUND_BRIGHT("\033[0;100m"), // BLACK
		RED_BACKGROUND_BRIGHT("\033[0;101m"), // RED
		GREEN_BACKGROUND_BRIGHT("\033[0;102m"), // GREEN
		YELLOW_BACKGROUND_BRIGHT("\033[0;103m"), // YELLOW
		BLUE_BACKGROUND_BRIGHT("\033[0;104m"), // BLUE
		MAGENTA_BACKGROUND_BRIGHT("\033[0;105m"), // MAGENTA
		CYAN_BACKGROUND_BRIGHT("\033[0;106m"), // CYAN
		WHITE_BACKGROUND_BRIGHT("\033[0;107m");     // WHITE

		private final String code;


		Color(String aCode)
		{
			code = aCode;
		}


		@Override
		public String toString()
		{
			return code;
		}
	}


	public static class ConsoleIP
	{
		public ConsoleP indent(int aIndent)
		{
			intentImpl(aIndent);
			return new ConsoleP();
		}


		public void println(Object... aArguments)
		{
			Console.println(aArguments);
		}
	}


	public static class ConsoleP
	{
		public void println(Object... aArguments)
		{
			Console.println(aArguments);
		}
	}


	public static ConsoleIP location()
	{
		int LEN = 40;

		StackTraceElement el = new Exception().getStackTrace()[1];
		String path = el.getClassName() + "." + el.getMethodName() + ":" + el.getLineNumber();
		path = trimPath(path, LEN);
		System.out.printf("%s[%s]%s [%-" + LEN + "s] ", Color.YELLOW_FAINT, DATE_FORMAT.format(System.currentTimeMillis()), Color.CYAN_FAINT, path);
		return new ConsoleIP();
	}


	private static String trimPath(String aPath, int aLimit)
	{
		if (aPath.length() > aLimit)
		{
			for (int i = 0; aPath.length() > aLimit; i += 2)
			{
				int j = aPath.indexOf('.', i);
				if (j == -1)
				{
					break;
				}
				int rem = 0;//Math.max(0, aLimit - (i + 1 + aPath.length() - j));
				aPath = aPath.substring(0, i + 1 + rem) + aPath.substring(j);
			}
			if (aPath.length() > aLimit)
			{
				aPath = aPath.substring(aPath.length() - aLimit);
			}
			if (aPath.startsWith("."))
			{
				aPath = aPath.substring(1);
			}
		}
		return aPath;
	}


	public static ConsoleP indent(int aIndent)
	{
		intentImpl(aIndent);
		return new ConsoleP();
	}


	private static void intentImpl(int aIndent)
	{
		System.out.print(Color.BLACK_BOLD + "... ".repeat(Math.min(Math.max(aIndent, 0), 20)));
	}


	public static void println(Object... aArguments)
	{
		try
		{
			for (int i = 0; i < aArguments.length; i++)
			{
				if (aArguments[i].toString().endsWith(":"))
				{
					if (i > 0)
					{
						System.out.print(Color.WHITE + ", ");
					}
					System.out.print(Color.BLACK + "" + aArguments[i] + " ");
					i++;
				}

				if (aArguments[i] instanceof Array v)
				{
					printArray(v);
				}
				else if (aArguments[i] instanceof Document v)
				{
					printDocument(v);
				}
				else if (aArguments[i] instanceof String v)
				{
					if (i > 0)
					{
						System.out.print(" ");
					}
					System.out.print(Color.WHITE + v);
				}
				else
				{
					printValue(aArguments[i]);
				}
			}

			System.out.println();
		}
		catch (Exception e)
		{
		}
	}


	private static void printArray(Array arg)
	{
		System.out.print(Color.WHITE + "[");
		for (int i = 0; i < arg.size(); i++)
		{
			if (i > 0)
			{
				System.out.print(Color.WHITE + ", ");
			}

			boolean percent = false;
			if (arg.get(i).toString().endsWith(":"))
			{
				String key = arg.get(i).toString();
				percent = key.endsWith("%:");
				if (percent)
				{
					key = key.substring(0, key.length() - 2) + ":";
				}
				System.out.print(Color.BLACK + "" + key + " ");
				i++;
			}

			if (arg.get(i) instanceof Array u)
			{
				printArray(u);
			}
			else if (arg.get(i) instanceof Document u)
			{
				printDocument(u);
			}
			else
			{
				printValue(arg.get(i));
				if (percent)
				{
					System.out.print("%");
				}
			}
		}
		System.out.print(Color.WHITE + "]");
	}


	private static void printDocument(Document arg)
	{
		boolean first = true;
		System.out.print(Color.WHITE + "{");
		for (Entry<String, Object> entry : arg.entrySet())
		{
			if (!first)
			{
				System.out.print(Color.WHITE + ", ");
			}
			first = false;

			String key = entry.getKey();
			boolean percent = key.endsWith("%");
			if (percent)
			{
				key = key.substring(0, key.length() - 1);
			}
			System.out.print(Color.BLACK + "" + key + ": ");

			if (entry.getValue() instanceof Array u)
			{
				printArray(u);
			}
			else if (entry.getValue() instanceof Document u)
			{
				printDocument(u);
			}
			else
			{
				printValue(entry.getValue());
				if (percent)
				{
					System.out.print("%");
				}
			}
		}
		System.out.print(Color.WHITE + "}");
	}


	private static void printValue(Object arg)
	{
		String s
			= switch (arg)
		{
			case Boolean v ->
				Color.GREEN + "" + v;
			case Float v ->
				Color.YELLOW + "" + String.format(Locale.US, "%5.2f", v);
			case Double v ->
				Color.YELLOW + "" + String.format(Locale.US, "%5.2f", v);
			case Number v ->
				Color.CYAN + "" + v;
			case String v ->
				Color.MAGENTA + "" + v;
			default ->
				arg == null ? "null" : arg.toString();
		};
		System.out.print(s);
	}


	public static void main(String... args)
	{
		try
		{
			Console.location().indent(4).println("aa", Array.of("aa:", 2, "bb:", "dummy", "cc:", 5.5, "bool:", true, "arr:", Array.of("aa:", 2, "bb:", "dummy", "cc:", 5.5, "bool:", true)));
			Console.indent(4).println("something", "more", "array:", Array.of("aa:", 2, "bb:", "dummy", "cc:", 5.5, "bool:", true, "arr:", Array.of("aa:", 2, "bb:", "dummy", "cc%:", 5.5, "bool:", true), "document:", Document.of("a%", 1, "b", "B", "c", true, "d", 5.3)));

//			for (int i = 45; --i >= 5;)
//			{
//				System.out.println(trimPath("org.terifan.raccoon.util.Console.printDocument:283", i));
//			}
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
	}
}
