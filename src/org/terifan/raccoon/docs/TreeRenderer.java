package org.terifan.raccoon.docs;

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.Stroke;
import java.awt.font.FontRenderContext;
import java.awt.font.LineMetrics;
import java.awt.geom.Rectangle2D;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.PushbackReader;
import java.io.StringReader;
import java.util.ArrayList;
import javax.swing.JFrame;
import javax.swing.JPanel;


public class TreeRenderer
{
	private final static BasicStroke LINE_STROKE = new BasicStroke(2.5f, BasicStroke.CAP_BUTT, BasicStroke.JOIN_BEVEL, 1f, new float[]
	{
		2f
	}, 0);
	private final static Font FONT = new Font("arial", Font.PLAIN, 14);
	private static final FontRenderContext FRC = new FontRenderContext(null, true, false);
	private final static LineMetrics LM = FONT.getLineMetrics("Adgjy", FRC);
	private final static int HOR_SPACING = 20;
	private final static int VER_SPACING = 50;
	private final static int IPADDING_X = 0;
	private final static int IPADDING_Y = 15;
	private final static int ELEMENT_SPACING = 11;
	private final static int FRAME_PADDING = 20;


	public static void main(String... args)
	{
		try
		{
			ArrayList<BufferedImage> images = new ArrayList<>();

			images.add(render(parse("'Japanese:Quality:>'['Dove:>'['Apple:Banana:Circus','Dove:Ear:Female'],'Japanese:>'['Gloves:Head:Internal','Japanese:Knife:Leap'],'Quality:>'['Mango:Nose:Open','Quality:Rupee']]")));

			//images.add(render(parse("'J:Q:>'['D:>'['A:B:C','D:E:F'],'J:>'['G:H:I','J:K:L'],'Q:>'['M:N:O','Q:R']]")));

			images.add(render(
				new Node("J", "O", ">")
					.add(new Node("D", ">")
						.add(new Node("A", "B", "C"))
						.add(new Node("D", "E", "F"))
					)
					.add(new Node("J", ">")
						.add(new Node("G", "H", "I"))
						.add(new Node("J", "K", "L"))
					)
					.add(new Node("O", "Q", ">")
						.add(new Node("M", "N"))
						.add(new Node("O", "P"))
						.add(new Node("Q", "R"))
					)
			));

			JFrame frame = new JFrame();
			frame.add(new JPanel()
			{
				@Override
				protected void paintComponent(Graphics aGraphics)
				{
					int y = 10;
					for (BufferedImage image : images)
					{
						aGraphics.drawImage(image, (getWidth() - image.getWidth()) / 2, y, null);
						y += image.getHeight() + 20;
					}
				}


				@Override
				public Dimension getPreferredSize()
				{
					int w = 0;
					int h = -20;
					for (BufferedImage image : images)
					{
						w = Math.max(image.getWidth(), w);
						h += image.getHeight() + 20;
					}
					return new Dimension(20 + w, 20 + h);
				}
			});
			frame.pack();
			frame.setLocationRelativeTo(null);
			frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
			frame.setVisible(true);
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
	}


	private static Node parse(String aInput) throws IOException
	{
		return parse(new PushbackReader(new StringReader(aInput)));
	}


	private static Node parse(PushbackReader aInput) throws IOException
	{
		int c = aInput.read();
		if (c != '\'')
		{
			throw new IllegalStateException();
		}

		String s = "";
		for (; (c = aInput.read()) != '\'';)
		{
			s += (char)c;
		}

		Node node = new Node(s.split(":"));

		c = aInput.read();

		if (c == '[')
		{
			do
			{
				node.add(parse(aInput));
				c = aInput.read();
			}
			while (c == ',');
		}
		else
		{
			aInput.unread(c);
		}

		return node;
	}


	private static BufferedImage render(Node aNode)
	{
		Dimension d = aNode.layout();

		BufferedImage image = new BufferedImage(d.width + 2 * FRAME_PADDING, d.height + 2 * FRAME_PADDING, BufferedImage.TYPE_INT_RGB);

		Graphics2D g = image.createGraphics();
		g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
		g.setRenderingHint(RenderingHints.KEY_TEXT_ANTIALIASING, RenderingHints.VALUE_TEXT_ANTIALIAS_LCD_HRGB);
		g.setFont(FONT);

		g.setColor(Color.WHITE);
		g.fillRect(0, 0, image.getWidth(), image.getHeight());

		aNode.render(g, FRAME_PADDING, FRAME_PADDING);

		g.dispose();

		return image;
	}


	private static class Node
	{
		private Dimension mLayout;
		private ArrayList<Node> mChildren;
		private String[] mText;
		private int mWidth;
		private int mHeight;
		private int mTextWidth;
		private int mTextHeight;


		public Node(String... aText)
		{
			mText = aText;
		}


		public Node add(Node aNode)
		{
			if (mChildren == null)
			{
				mChildren = new ArrayList<>();
			}
			mChildren.add(aNode);
			return this;
		}


		public Dimension layout()
		{
			mTextWidth = -ELEMENT_SPACING;
			mTextHeight = 0;
			for (String s : mText)
			{
				Rectangle2D b = FONT.getStringBounds(s, FRC);
				mTextWidth += b.getWidth() + ELEMENT_SPACING;
				mTextHeight = Math.max((int)b.getHeight(), mTextHeight);
			}
			mWidth = IPADDING_X + mTextWidth + ELEMENT_SPACING + 2;
			mHeight = IPADDING_Y + mTextHeight;

			if (mChildren == null)
			{
				mLayout = new Dimension(mWidth, mHeight);
			}
			else
			{
				int w = -HOR_SPACING;
				int h = 0;
				for (Node n : mChildren)
				{
					Dimension d = n.layout();
					w += d.width + HOR_SPACING;
					h = Math.max(d.height, h);
				}

				mLayout = new Dimension(Math.max(w, mWidth), h + VER_SPACING + mHeight);
			}

			return mLayout;
		}


		private void render(Graphics2D aGraphics, int aX, int aY)
		{
			int x = aX + (mLayout.width - mWidth) / 2;

			aGraphics.setColor(Color.BLACK);
			aGraphics.drawRect(x, aY, mWidth - 1, mHeight - 1);

			int tx = x + (mWidth - mTextWidth) / 2;
			for (int i = 0; i < mText.length; i++)
			{
				int w = (int)FONT.getStringBounds(mText[i], FRC).getWidth();
				aGraphics.setColor(Color.BLACK);
				aGraphics.drawString(mText[i], tx, aY + (mHeight + LM.getHeight()) / 2 - aGraphics.getFontMetrics().getDescent());
				if (i > 0)
				{
					aGraphics.setColor(Color.LIGHT_GRAY);
					aGraphics.drawLine(tx-ELEMENT_SPACING/2, aY+1, tx-ELEMENT_SPACING/2, aY+mHeight-2);
				}
				tx += w + ELEMENT_SPACING;
			}

			if (mChildren != null)
			{
				Stroke oldStroke = aGraphics.getStroke();

				int x0 = x;
				int step = mWidth / mChildren.size();
				for (Node n : mChildren)
				{
					n.render(aGraphics, aX, aY + mHeight + VER_SPACING);

					int y0 = aY + mHeight;
					int x1 = aX + n.mLayout.width / 2;
					int y1 = aY + mHeight + VER_SPACING - 1;
					aGraphics.setColor(Color.LIGHT_GRAY);
					aGraphics.setStroke(LINE_STROKE);
					aGraphics.drawLine(x0 + step / 2, y0 + 5, x1, y1 - 5);
					aGraphics.setStroke(oldStroke);

					aX += n.mLayout.width + HOR_SPACING;
					x0 += step;
				}
			}
		}
	}
}
