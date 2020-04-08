package org.terifan.raccoon.util;

import java.awt.Color;
import java.awt.Dimension;
import java.awt.Graphics2D;
import java.awt.Rectangle;
import java.util.ArrayList;
import java.util.function.Function;


public class GraphTreeRenderer<T extends GraphTreeNode>
{
	static int VS = 40;
	static int HS = 10;
	static Dimension IP = new Dimension(20, 6);


	public void render(Graphics2D aGraphics, T aRoot, Function<T, T[]> aProvider)
	{
		T[] children = aProvider.apply(aRoot);

		Box box = computeSize(aGraphics, aRoot, children, aProvider);
		box.layout(0, 0);
		box.render(aGraphics);
	}


	private Box computeSize(Graphics2D aGraphics, T aNode, T[] aChildren, Function<T, T[]> aProvider)
	{
		Box box = new Box(aNode == null ? "root" : "" + aNode.getLabel());
		box.mSize.width = aGraphics.getFontMetrics().stringWidth(box.mLabel) + IP.width;
		box.mSize.height = aGraphics.getFontMetrics().getHeight() + IP.height;

		for (T child : aChildren)
		{
			if (child != null)
			{
				T[] grandChildren = aProvider.apply(child);

				if (grandChildren != null && grandChildren.length > 0)
				{
					Box b = computeSize(aGraphics, child, grandChildren, aProvider);
					box.mBounds.width += b.mBounds.width + HS;
					box.mBounds.height = Math.max(box.mBounds.height, b.mBounds.height);
					box.mChildren.add(b);
				}
				else
				{
					Box b = new Box("" + child.getLabel());
					b.mSize.width = aGraphics.getFontMetrics().stringWidth(b.mLabel) + IP.width;
					b.mSize.height = aGraphics.getFontMetrics().getHeight() + IP.height;
					b.mBounds.width = b.mSize.width + HS;
					b.mBounds.height = b.mSize.height;
					box.mChildren.add(b);

					box.mBounds.width += b.mBounds.width;
					box.mBounds.height = Math.max(box.mBounds.height, b.mBounds.height);
				}
			}
		}

		box.mChildrenWidth = box.mBounds.width;
		box.mBounds.width = Math.max(box.mBounds.width, box.mSize.width);
		box.mBounds.height += box.mSize.height;

		return box;
	}


	private static class Box
	{
		String mLabel;
		ArrayList<Box> mChildren = new ArrayList<>();
		Rectangle mBounds = new Rectangle();
		Dimension mSize = new Dimension();
		int mChildrenWidth;


		public Box(String aLabel)
		{
			mLabel = aLabel;
		}


		void layout(int aStartX, int aStartY)
		{
			mBounds.x = aStartX;
			mBounds.y = aStartY;

			int x = aStartX + (mBounds.width - mChildrenWidth) / 2;
			int y = aStartY + mSize.height + VS;

			for (Box box : mChildren)
			{
				box.layout(x, y);

				x += box.mBounds.width;
			}
		}


		void render(Graphics2D aGraphics)
		{
			if (mChildren.isEmpty())
			{
				aGraphics.setColor(Color.BLUE);
				aGraphics.drawRect(mBounds.x + (mBounds.width - mSize.width) / 2, mBounds.y + 10, mSize.width, mSize.height);
			}
			else
			{
				aGraphics.setColor(Color.BLACK);
				aGraphics.drawRect(mBounds.x + (mBounds.width - mSize.width) / 2, mBounds.y + 10, mSize.width, mSize.height);
			}

			if (mLabel != null)
			{
				aGraphics.drawString("" + mLabel, mBounds.x + (mBounds.width - aGraphics.getFontMetrics().stringWidth(mLabel)) / 2, mBounds.y + 10 + IP.height / 2 + mSize.height / 2 + aGraphics.getFontMetrics().getHeight() - aGraphics.getFontMetrics().getAscent());
			}

			for (int i = 0, cs = mChildren.size(), hcs = cs / 2; i < cs; i++)
			{
				Box box = mChildren.get(i);

				aGraphics.setColor(Color.LIGHT_GRAY);

				int x0 = mBounds.x + mBounds.width / 2 - mSize.width / 2 + mSize.width / 2 / cs + mSize.width * i / cs;
				int y0 = mBounds.y + 10 + mSize.height + 1;
				int x1 = box.mBounds.x + box.mBounds.width / 2;
				int y1 = box.mBounds.y + 10;

				aGraphics.drawLine(x0, y0, x1, y1);

//				int yc = i < cs / 2 ? y0 + VS / 2 / hcs + (y1 - y0) * i / hcs : i > cs / 2 ? y0 + VS / 2 / hcs + (y1 - y0) * (cs - 1 - i) / hcs : (y1 + y0) / 2;
//				aGraphics.drawLine(x0, y0, x0, yc);
//				aGraphics.drawLine(x0, yc, x1, yc);
//				aGraphics.drawLine(x1, yc, x1, y1);

				box.render(aGraphics);
			}
		}
	}
}