package org.terifan.raccoon.util;

import org.terifan.raccoon.BlockType;


public class Console
{
	enum Color
	{
		// Color end string, color reset
		RESET("\033[0m"),

		// Regular Colors. Normal color, no bold, background color etc.
		BLACK("\033[0;30m"),    // BLACK
		RED("\033[0;31m"),      // RED
		GREEN("\033[0;32m"),    // GREEN
		YELLOW("\033[0;33m"),   // YELLOW
		BLUE("\033[0;34m"),     // BLUE
		MAGENTA("\033[0;35m"),  // MAGENTA
		CYAN("\033[0;36m"),     // CYAN
		WHITE("\033[0;37m"),    // WHITE

		// Bold
		BLACK_LIGHT("\033[1;30m"),   // BLACK
		RED_LIGHT("\033[1;31m"),     // RED
		GREEN_LIGHT("\033[1;32m"),   // GREEN
		YELLOW_LIGHT("\033[1;33m"),  // YELLOW
		BLUE_LIGHT("\033[1;34m"),    // BLUE
		MAGENTA_LIGHT("\033[1;35m"), // MAGENTA
		CYAN_LIGHT("\033[1;36m"),    // CYAN
		WHITE_LIGHT("\033[1;37m"),   // WHITE

		// Dark
		BLACK_DARK("\033[2;30m"),   // BLACK
		RED_DARK("\033[2;31m"),     // RED
		GREEN_DARK("\033[2;32m"),   // GREEN
		YELLOW_DARK("\033[2;33m"),  // YELLOW
		BLUE_DARK("\033[2;34m"),    // BLUE
		MAGENTA_DARK("\033[2;35m"), // MAGENTA
		CYAN_DARK("\033[2;36m"),    // CYAN
		WHITE_DARK("\033[2;37m"),   // WHITE

		// Underline
		BLACK_UNDERLINED("\033[4;30m"),     // BLACK
		RED_UNDERLINED("\033[4;31m"),       // RED
		GREEN_UNDERLINED("\033[4;32m"),     // GREEN
		YELLOW_UNDERLINED("\033[4;33m"),    // YELLOW
		BLUE_UNDERLINED("\033[4;34m"),      // BLUE
		MAGENTA_UNDERLINED("\033[4;35m"),   // MAGENTA
		CYAN_UNDERLINED("\033[4;36m"),      // CYAN
		WHITE_UNDERLINED("\033[4;37m"),     // WHITE

		// Background
		BLACK_BACKGROUND("\033[40m"),   // BLACK
		RED_BACKGROUND("\033[41m"),     // RED
		GREEN_BACKGROUND("\033[42m"),   // GREEN
		YELLOW_BACKGROUND("\033[43m"),  // YELLOW
		BLUE_BACKGROUND("\033[44m"),    // BLUE
		MAGENTA_BACKGROUND("\033[45m"), // MAGENTA
		CYAN_BACKGROUND("\033[46m"),    // CYAN
		WHITE_BACKGROUND("\033[47m"),   // WHITE

		// High Intensity
		BLACK_BRIGHT("\033[0;90m"),     // BLACK
		RED_BRIGHT("\033[0;91m"),       // RED
		GREEN_BRIGHT("\033[0;92m"),     // GREEN
		YELLOW_BRIGHT("\033[0;93m"),    // YELLOW
		BLUE_BRIGHT("\033[0;94m"),      // BLUE
		MAGENTA_BRIGHT("\033[0;95m"),   // MAGENTA
		CYAN_BRIGHT("\033[0;96m"),      // CYAN
		WHITE_BRIGHT("\033[0;97m"),     // WHITE

		// Bold High Intensity
		BLACK_BOLD_BRIGHT("\033[1;90m"),    // BLACK
		RED_BOLD_BRIGHT("\033[1;91m"),      // RED
		GREEN_BOLD_BRIGHT("\033[1;92m"),    // GREEN
		YELLOW_BOLD_BRIGHT("\033[1;93m"),   // YELLOW
		BLUE_BOLD_BRIGHT("\033[1;94m"),     // BLUE
		MAGENTA_BOLD_BRIGHT("\033[1;95m"),  // MAGENTA
		CYAN_BOLD_BRIGHT("\033[1;96m"),     // CYAN
		WHITE_BOLD_BRIGHT("\033[1;97m"),    // WHITE

		// High Intensity backgrounds
		BLACK_BACKGROUND_BRIGHT("\033[0;100m"),     // BLACK
		RED_BACKGROUND_BRIGHT("\033[0;101m"),       // RED
		GREEN_BACKGROUND_BRIGHT("\033[0;102m"),     // GREEN
		YELLOW_BACKGROUND_BRIGHT("\033[0;103m"),    // YELLOW
		BLUE_BACKGROUND_BRIGHT("\033[0;104m"),      // BLUE
		MAGENTA_BACKGROUND_BRIGHT("\033[0;105m"),   // MAGENTA
		CYAN_BACKGROUND_BRIGHT("\033[0;106m"),      // CYAN
		WHITE_BACKGROUND_BRIGHT("\033[0;107m");     // WHITE// WHITE

		private final String code;


		Color(String code)
		{
			this.code = code;
		}


		@Override
		public String toString()
		{
			return code;
		}
	}

	private static boolean mEnabled = true;

	public static void setPrettyColorsEnabled(boolean aEnabled)
	{
		mEnabled = aEnabled;
	}


	public static void printf(Object aText, Object... aParams)
	{
		System.out.printf(format(aText, aParams));
	}


	public static void println(Object aText, Object... aParams)
	{
		System.out.println(format(aText, aParams));
	}


	public static void repeat(int aLevel, Object aText)
	{
		for (int i = 0; i < aLevel; i++)
		{
			System.out.print(aText);
		}
	}


	public static String format(Object aText, Object... aParams)
	{
		String text = aText.toString();

//		if (mEnabled)
//		{
//			for (int i = text.length() - 1;;)
//			{
//				i = text.lastIndexOf('=', i);
//				if (i == -1) break;
//				int j = Math.max(Math.max(text.lastIndexOf(' ', i), text.lastIndexOf('(', i)), text.lastIndexOf('{', i));
//				if (j != -1)
//				{
//					j++;
//					text = text.substring(0, j) + Color.BLACK_LIGHT + text.substring(j,i) + Color.RESET + text.substring(i);
//				}
//				i = j;
//			}
//
//			text = text
//				.replace("%s", Color.BLUE + "%s" + Color.RESET)
//				.replace("#%d", Color.MAGENTA + "#%d")
//				.replace("%d", Color.MAGENTA + "%d" + Color.RESET)
//				.replace("%f", Color.MAGENTA + "%f" + Color.RESET);
//		}

		return String.format(text, aParams);
	}
}
