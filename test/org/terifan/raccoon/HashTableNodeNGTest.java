package org.terifan.raccoon;

import java.awt.Color;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import javax.swing.JFrame;
import javax.swing.JPanel;
import org.terifan.raccoon.storage.BlockPointer;
import org.terifan.raccoon.storage.IBlockAccessor;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.GraphTreeNode;
import org.terifan.raccoon.util.GraphTreeRenderer;
import static org.testng.Assert.*;
import org.testng.annotations.Test;


public class HashTableNodeNGTest
{
	@Test
	public void testPointersString()
	{
		HashTableNode node = (HashTableNode)createNode(null, new int[][]
		{
			{
				0, 9789, 0, 4
			},
			{
				1, 3467, 4, 2
			},
			{
				0, 5679, 6, 1
			},
			{
				0, 2349, 7, 1
			}
		});

		assertEquals(node.toPointersString(), "[9789,0,0,0],[*3467,0],[5679],[2349]");
	}


	@Test
	public void testReadNode()
	{
		HashTableAbstractNode root = createSampleTree();

		JFrame frame = new JFrame();
		frame.add(new JPanel()
		{
			@Override
			protected void paintComponent(Graphics aGraphics)
			{
				Graphics2D g = (Graphics2D)aGraphics;
				g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
				g.setColor(Color.WHITE);
				g.fillRect(0, 0, getWidth(), getHeight());

				new GraphTreeRenderer<Node>().render(g, new Node(root, new BlockPointer()), Node::children);
			}
		});
		frame.setSize(1024, 768);
		frame.setLocationRelativeTo(null);
		frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		frame.setVisible(true);

		try
		{
			Thread.sleep(100000);
		}
		catch (Exception e)
		{
			e.printStackTrace(System.out);
		}

//		assertEquals(node.printRanges(), "[97897,0,0,0],[3467,0],[5679],[2349]");
	}

	private static class Node extends GraphTreeNode
	{
		HashTableAbstractNode mNode;

		private Node(HashTableAbstractNode aNode, BlockPointer aBlockPointer)
		{
			mNode = aNode;
			mLabel = aBlockPointer.getBlockIndex0() + ": " + mNode.toString();
		}

		Node[] children()
		{
			if (mNode instanceof HashTableNode)
			{
				HashTableNode node = (HashTableNode)mNode;
				Node[] nodes = new Node[node.getPointerCount()];
				for (int i = 0; i < node.getPointerCount(); i++)
				{
					BlockPointer ptr = node.getPointer(i);
					if (ptr != null)
					{
						nodes[i] = new Node(node.readBlock(ptr), ptr);
					}
				}
				return nodes;
			}

			return null;
		}
	}


	private HashTableAbstractNode createSampleTree()
	{
		final HashTableAbstractNode[] nodes = new HashTableAbstractNode[14];

		IBlockAccessor ba = new IBlockAccessor()
		{
			@Override
			public void freeBlock(BlockPointer aBlockPointer)
			{
				throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
			}


			@Override
			public byte[] readBlock(BlockPointer aBlockPointer)
			{
				return nodes[(int)aBlockPointer.getBlockIndex0()].array();
			}


			@Override
			public BlockPointer writeBlock(byte[] aBuffer, int aOffset, int aLength, long aTransactionId, BlockType aType, int aRangeOffset, int aRangeSize, int aLevel)
			{
				throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
			}
		};

		HashTableAbstractNode[] tmp =
		{
			createNode(ba, new int[][]
			{
				{
					0, 1, 0, 4
				},
				{
					1, 2, 4, 2
				},
				{
					0, 3, 6, 1
				},
				{
					0, 4, 7, 1
				}
			}),
			createNode(ba, new int[][]
			{
				{
					1, 5, 0, 4
				},
				{
					0, 6, 4, 4
				}
			}),
			createNode(ba),
			createNode(ba, new int[][]
			{
				{
					1, 7, 0, 2
				},
				{
					1, 8, 2, 4
				},
				{
					1, 9, 6, 2
				}
			}),
			createNode(ba, new int[][]
			{
				{
					1, 10, 0, 4
				},
				{
					1, 11, 4, 4
				}
			}),
			createNode(ba),
			createNode(ba, new int[][]
			{
				{
					1, 12, 0, 7
				},
				{
					1, 13, 7, 1
				}
			}),
			createNode(ba),
			createNode(ba),
			createNode(ba),
			createNode(ba),
			createNode(ba),
			createNode(ba),
			createNode(ba)
		};

		System.arraycopy(tmp, 0, nodes, 0, tmp.length);

		return nodes[0];
	}


	private HashTableAbstractNode createNode(IBlockAccessor aAccessor, int[]... aParams)
	{
		if (aParams.length == 0)
		{
			return new HashTableLeaf(null, aAccessor, null, new byte[8 * BlockPointer.SIZE]);
		}

		ByteArrayBuffer buf = new ByteArrayBuffer(8 * BlockPointer.SIZE);

		for (int[] params : aParams)
		{
			createPointer(params[0] == 1 ? BlockType.LEAF : BlockType.INDEX, params[1], params[2], params[3], buf);
		}

		return new HashTableNode(null, aAccessor, null, buf.array());
	}


	private BlockPointer createPointer(BlockType aBlockType, long aBlockIndex, int aRangeOffset, int aRangeSize, ByteArrayBuffer aBuffer)
	{
		BlockPointer bp = new BlockPointer();
		bp.setBlockType(aBlockType);
		bp.setAllocatedSize(1);
		bp.setLevel(1);
		bp.setLogicalSize(1000);
		bp.setPhysicalSize(1000);
		bp.setBlockIndex0(aBlockIndex);
		bp.setRangeOffset(aRangeOffset);
		bp.setRangeSize(aRangeSize);

		bp.marshal(aBuffer.position(aRangeOffset * BlockPointer.SIZE));

		return bp;
	}
}
