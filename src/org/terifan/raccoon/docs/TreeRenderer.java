package org.terifan.raccoon.docs;

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.GridLayout;
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
	private final static FontRenderContext FRC = new FontRenderContext(null, true, false);
	private final static LineMetrics LM = FONT.getLineMetrics("Adgjy", FRC);
	private final static int SIBLING_SPACING = 50;
	private final static int CHILD_SPACING = 20;
	private final static int TEXT_PADDING_X = 15;
	private final static int TEXT_PADDING_Y = 11;
	private final static int FRAME_PADDING = 20;
	private final static boolean COMPACT_LEAFS = true;


	public static void main(String... args)
	{
		try
		{
			ArrayList<BufferedImage> images = new ArrayList<>();
			images.add(render(new HorizontalLayout(), parse("'Jalapeno:Quality:>'['Dove:>'['Apple:Banana:Circus','Dove:Ear:Female'],'Japanese:>'['Gloves:Head:Internal','Japanese:Knife:Leap'],'Quality:>'['Mango:Nose:Open','Quality:Rupee']]")));
			images.add(render(new VerticalLayout(), parse("'Jalapeno:Quality:>'['Dove:>'['Apple:Banana:Circus','Dove:Ear:Female'],'Japanese:>'['Gloves:Head:Internal','Japanese:Knife:Leap'],'Open:Quality:>'['Mango:Nose','Open:Pen','Quality:Rupee']]")));

			JFrame frame = new JFrame();
			frame.setLayout(new GridLayout(1, 1));
			for (BufferedImage image : images)
			{
				frame.add(new JPanel()
				{
					@Override
					protected void paintComponent(Graphics aGraphics)
					{
						int y = 10;
						aGraphics.drawImage(image, (getWidth() - image.getWidth()) / 2, y, null);
						y += image.getHeight() + 20;
					}
					@Override
					public Dimension getPreferredSize()
					{
						int w = 0;
						int h = -20;
						w = Math.max(image.getWidth(), w);
						h += image.getHeight() + 20;
						return new Dimension(20 + w, 20 + h);
					}
				});
			}
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
		if (aInput.read() != '\'')
		{
			throw new IllegalArgumentException();
		}

		StringBuilder s = new StringBuilder();
		for (int c; (c = aInput.read()) != '\'';)
		{
			s.append((char)c);
		}

		Node node = new Node(s.toString().split(":"));
		int c = aInput.read();
		if (c == '[')
		{
			do
			{
				node.add(parse(aInput));
			}
			while (aInput.read() == ',');
		}
		else
		{
			aInput.unread(c);
		}

		return node;
	}


	private static BufferedImage render(NodeLayout aLayout, Node aNode)
	{
		if (aNode.mLayout != aLayout)
		{
			aNode.mLayout = aLayout;
			aLayout.layout(aNode);
		}

		Dimension d = aNode.mLayout.mBounds;

		BufferedImage image = new BufferedImage(d.width + 2 * FRAME_PADDING, d.height + 2 * FRAME_PADDING, BufferedImage.TYPE_INT_RGB);

		Graphics2D g = image.createGraphics();
		g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
		g.setRenderingHint(RenderingHints.KEY_TEXT_ANTIALIASING, RenderingHints.VALUE_TEXT_ANTIALIAS_LCD_HRGB);
		g.setFont(FONT);
		g.setColor(Color.WHITE);
		g.fillRect(0, 0, image.getWidth(), image.getHeight());

		aLayout.render(aNode, g, FRAME_PADDING, FRAME_PADDING);

		g.dispose();

		return image;
	}


	private static class Node
	{
		private ArrayList<Node> mChildren;
		private String[] mText;
		private NodeLayout mLayout;


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
	}


	private abstract class NodeLayout
	{
		Dimension mBounds;
		int mWidth;
		int mHeight;
		int mTextWidth;
		int mTextHeight;


		abstract void layout(Node aNode);


		abstract void render(Node aNode, Graphics2D aGraphics, int aX, int aY);
	}


	private static void renderHorizontalBox(Graphics2D aGraphics, int aX, int aY, Node aNode)
	{
		aGraphics.setColor(Color.BLACK);
		aGraphics.drawRect(aX, aY, aNode.mLayout.mWidth - 1, aNode.mLayout.mHeight - 1);
		for (int i = 0, tx = aX + (aNode.mLayout.mWidth - aNode.mLayout.mTextWidth) / 2; i < aNode.mText.length; i++)
		{
			aGraphics.setColor(Color.BLACK);
			aGraphics.drawString(aNode.mText[i], tx, aY + (aNode.mLayout.mHeight + LM.getHeight()) / 2 - aGraphics.getFontMetrics().getDescent());
			if (i > 0)
			{
				aGraphics.setColor(Color.LIGHT_GRAY);
				aGraphics.drawLine(tx - TEXT_PADDING_X / 2, aY + 1, tx - TEXT_PADDING_X / 2, aY + aNode.mLayout.mHeight - 2);
			}
			tx += FONT.getStringBounds(aNode.mText[i], FRC).getWidth() + TEXT_PADDING_X;
		}
	}


	private static void renderVerticalBox(Graphics2D aGraphics, int aX, int aY, Node aNode)
	{
		aGraphics.setColor(Color.BLACK);
		aGraphics.drawRect(aX, aY, aNode.mLayout.mWidth - 1, aNode.mLayout.mHeight - 1);
		for (int i = 0, ty = aY + (aNode.mLayout.mHeight - aNode.mLayout.mTextHeight) / 2; i < aNode.mText.length; i++)
		{
			aGraphics.setColor(Color.BLACK);
			aGraphics.drawString(aNode.mText[i], aX + (aNode.mLayout.mWidth - aGraphics.getFontMetrics().stringWidth(aNode.mText[i])) / 2, ty + LM.getHeight() - aGraphics.getFontMetrics().getDescent());
			if (i > 0)
			{
				aGraphics.setColor(Color.LIGHT_GRAY);
				aGraphics.drawLine(aX + 1, ty - TEXT_PADDING_Y / 2, aX + aNode.mLayout.mWidth - 2, ty - TEXT_PADDING_Y / 2);
			}
			ty += LM.getHeight() + TEXT_PADDING_Y;
		}
	}


	private static void layoutHorizontalBox(Node aNode)
	{
		aNode.mLayout.mTextWidth = -TEXT_PADDING_X;
		aNode.mLayout.mTextHeight = 0;
		for (String s : aNode.mText)
		{
			Rectangle2D b = FONT.getStringBounds(s, FRC);
			aNode.mLayout.mTextWidth += b.getWidth() + TEXT_PADDING_X;
			aNode.mLayout.mTextHeight = Math.max((int)b.getHeight(), aNode.mLayout.mTextHeight);
		}
		aNode.mLayout.mWidth = TEXT_PADDING_X + aNode.mLayout.mTextWidth + 2;
		aNode.mLayout.mHeight = TEXT_PADDING_Y + aNode.mLayout.mTextHeight;
	}


	private static void layoutVerticalBox(Node aNode)
	{
		aNode.mLayout.mTextWidth = 0;
		aNode.mLayout.mTextHeight = -TEXT_PADDING_Y;
		for (String s : aNode.mText)
		{
			Rectangle2D b = FONT.getStringBounds(s, FRC);
			aNode.mLayout.mTextWidth = Math.max((int)b.getWidth(), aNode.mLayout.mTextWidth);
			aNode.mLayout.mTextHeight += b.getHeight() + TEXT_PADDING_Y;
		}
		aNode.mLayout.mWidth = TEXT_PADDING_X + aNode.mLayout.mTextWidth;
		aNode.mLayout.mHeight = TEXT_PADDING_Y + aNode.mLayout.mTextHeight + 2;
	}


	private static class HorizontalLayout extends NodeLayout
	{
		@Override
		public void layout(Node aNode)
		{
			if (aNode.mChildren == null)
			{
				if (COMPACT_LEAFS)
				{
					layoutVerticalBox(aNode);
				}
				else
				{
					layoutHorizontalBox(aNode);
				}

				mBounds = new Dimension(mWidth, mHeight);
			}
			else
			{
				layoutHorizontalBox(aNode);

				int w = -CHILD_SPACING;
				int h = 0;
				for (Node n : aNode.mChildren)
				{
					n.mLayout = new HorizontalLayout();
					n.mLayout.layout(n);
					w += n.mLayout.mBounds.width + CHILD_SPACING;
					h = Math.max(n.mLayout.mBounds.height, h);
				}

				mBounds = new Dimension(Math.max(w, mWidth), h + SIBLING_SPACING + mHeight);
			}
		}


		@Override
		public void render(Node aNode, Graphics2D aGraphics, int aX, int aY)
		{
			int x = aX + (mBounds.width - mWidth) / 2;

			if (aNode.mChildren != null)
			{
				renderHorizontalBox(aGraphics, x, aY, aNode);

				Stroke oldStroke = aGraphics.getStroke();

				boolean b = aNode.mText.length == aNode.mChildren.size();
				int t = b ? x + (mWidth - mTextWidth) / 2 : x;
				int s = mWidth / aNode.mChildren.size();
				int w = s;
				int ch = -CHILD_SPACING;
				for (int i = 0; i < aNode.mChildren.size(); i++)
				{
					ch += aNode.mChildren.get(i).mLayout.mBounds.width + CHILD_SPACING;
				}
				if (ch < mWidth)
				{
					aX += (mWidth - ch) / 2;
				}
				for (int i = 0; i < aNode.mChildren.size(); i++)
				{
					if (b)
					{
						w = (int)FONT.getStringBounds(aNode.mText[i], FRC).getWidth();
						s = w + TEXT_PADDING_X;
					}

					Node n = aNode.mChildren.get(i);
					n.mLayout.render(n, aGraphics, aX, aY + mHeight + SIBLING_SPACING);

					aGraphics.setColor(Color.LIGHT_GRAY);
					aGraphics.setStroke(LINE_STROKE);
					aGraphics.drawLine(t + w / 2, aY + mHeight + 5, aX + n.mLayout.mBounds.width / 2, aY + mHeight + SIBLING_SPACING - 5);
					aGraphics.setStroke(oldStroke);

					aX += n.mLayout.mBounds.width + CHILD_SPACING;
					t += s;
				}
			}
			else if (COMPACT_LEAFS)
			{
				renderVerticalBox(aGraphics, aX, aY, aNode);
			}
			else
			{
				renderHorizontalBox(aGraphics, aX, aY, aNode);
			}
		}
	}


	private static class VerticalLayout extends NodeLayout
	{
		@Override
		public void layout(Node aNode)
		{
			if (aNode.mChildren == null)
			{
				if (COMPACT_LEAFS)
				{
					layoutHorizontalBox(aNode);
				}
				else
				{
					layoutVerticalBox(aNode);
				}

				mBounds = new Dimension(mWidth, mHeight);
			}
			else
			{
				layoutVerticalBox(aNode);

				int w = 0;
				int h = -CHILD_SPACING;
				for (Node n : aNode.mChildren)
				{
					n.mLayout = new VerticalLayout();
					n.mLayout.layout(n);
					w = Math.max(n.mLayout.mBounds.width, w);
					h += n.mLayout.mBounds.height + CHILD_SPACING;
				}

				mBounds = new Dimension(w + SIBLING_SPACING + mWidth, Math.max(h, mHeight));
			}
		}


		@Override
		public void render(Node aNode, Graphics2D aGraphics, int aX, int aY)
		{
			int y = aY + (mBounds.height - mHeight) / 2;

			if (aNode.mChildren != null)
			{
				renderVerticalBox(aGraphics, aX, y, aNode);

				Stroke oldStroke = aGraphics.getStroke();

				boolean b = aNode.mText.length == aNode.mChildren.size();
				int t = b ? y + (mHeight - mTextHeight) / 2 : y;
				int s = mHeight / aNode.mChildren.size();
				int h = s;
				int ch = -CHILD_SPACING;
				for (int i = 0; i < aNode.mChildren.size(); i++)
				{
					ch += aNode.mChildren.get(i).mLayout.mBounds.height + CHILD_SPACING;
				}
				if (ch < mHeight)
				{
					aY += (mHeight - ch) / 2;
				}
				for (int i = 0; i < aNode.mChildren.size(); i++)
				{
					if (b)
					{
						h = (int)FONT.getStringBounds(aNode.mText[i], FRC).getHeight();
						s = h + TEXT_PADDING_Y;
					}

					Node n = aNode.mChildren.get(i);
					n.mLayout.render(n, aGraphics, aX + mWidth + SIBLING_SPACING, aY);

					aGraphics.setColor(Color.LIGHT_GRAY);
					aGraphics.setStroke(LINE_STROKE);
					aGraphics.drawLine(aX + mWidth + 5, t + h / 2, aX + mWidth + SIBLING_SPACING - 5, aY + n.mLayout.mBounds.height / 2);
					aGraphics.setStroke(oldStroke);

					aY += n.mLayout.mBounds.height + CHILD_SPACING;
					t += s;
				}
			}
			else if (COMPACT_LEAFS)
			{
				renderHorizontalBox(aGraphics, aX, aY, aNode);
			}
			else
			{
				renderVerticalBox(aGraphics, aX, aY, aNode);
			}
		}
	}
}
