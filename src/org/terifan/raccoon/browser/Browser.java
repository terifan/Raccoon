package org.terifan.raccoon.browser;

import java.awt.BorderLayout;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.File;
import java.util.Arrays;
import java.util.GregorianCalendar;
import java.util.Iterator;
import javax.swing.JFrame;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTabbedPane;
import javax.swing.JTable;
import javax.swing.JTree;
import javax.swing.UIManager;
import javax.swing.table.DefaultTableModel;
import javax.swing.tree.DefaultMutableTreeNode;
import org.terifan.raccoon.Database;
import org.terifan.raccoon.Entry;
import org.terifan.raccoon.OpenOption;
import org.terifan.raccoon.Table;
import org.terifan.raccoon.serialization.FieldType;
import org.terifan.raccoon.util.Log;
import tests._BigObject1K;
import tests._Fruit1K;
import tests._Number1K1D;
import tests._Number1K2D;
import tests._Object1K;


public class Browser
{
	public static void main(String ... args)
	{
		try
		{
			try (Database db = Database.open(new File("d:/sample.db"), OpenOption.CREATE_NEW))
			{
				db.save(new _Fruit1K("apple", 52.12));
				db.save(new _Fruit1K("orange", 47.78));
				db.save(new _Fruit1K("banana", 89.45));
				db.save(new _Number1K2D(1, "yellow", 89, "lemon"));
				db.save(new _Number1K2D(2, "green", 7, "apple"));
				db.save(new _Number1K2D(2, "red", 42, "apple"));
				db.save(new _Number1K2D(1, "yellow", 13, "banan"));
				db.save(new _Object1K("test", new GregorianCalendar()));
				db.save(new _Number1K1D("a", 1));
				db.save(new _Number1K1D("b", 2));
				db.save(new _Number1K1D("c", 3));
				db.save(new _Number1K1D("d", 4));
				db.save(new _BigObject1K().random());
				db.save(new _BigObject1K().random());
				db.save(new _BigObject1K().random());
				db.commit();
			}

			UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
			
			JFrame frame = new JFrame();

			final Database database = Database.open(new File("d:/sample.db"), OpenOption.OPEN);

			final DefaultTableModel tableContentmodel = new DefaultTableModel(new String[]{"Key","Value"}, 0);
			final DefaultTableModel tableFormatModel = new DefaultTableModel(new String[]{"Category","Name","Format","Type","Nullable","Component","Depth"}, 0);

			DefaultMutableTreeNode root = new DefaultMutableTreeNode();
			DefaultMutableTreeNode group = null;
			String typeName = null;
			for (Table table : database.getTables())
			{
				if (!table.getTableMetadata().getTypeName().equals(typeName))
				{
					if (table.getTableMetadata().hasDiscriminatorFields())
					{
						typeName = table.getTableMetadata().getTypeName();
						group = new DefaultMutableTreeNode(typeName);
						root.add(group);
					}
					else
					{
						group = null;
					}
				}

				if (group != null)
				{
					DefaultMutableTreeNode node = new DefaultMutableTreeNode(table)
					{
						@Override
						public String toString()
						{
							return ((Table)userObject).getTableMetadata().getDiscriminatorDescription();
						}
					};
					group.add(node);
				}
				else
				{
					DefaultMutableTreeNode node = new DefaultMutableTreeNode(table);
					root.add(node);
				}
			}
			
			JTree tableList = new JTree(root);
			tableList.setRootVisible(false);
			for (int i = 0; i < tableList.getRowCount(); i++)
			{
				tableList.expandRow(i);
			}
			
			JTable tableFormat = new JTable(tableFormatModel);
			JTable tableContent = new JTable(tableContentmodel);
			JScrollPane tableListScroll = new JScrollPane(tableList);
			JScrollPane tableContentScroll = new JScrollPane(tableContent);
			JScrollPane tableFormatScroll = new JScrollPane(tableFormat);
			JTabbedPane tabbedPane = new JTabbedPane();
			tabbedPane.add("Format", tableFormatScroll);
			tabbedPane.add("Content", tableContentScroll);

			tableList.addTreeSelectionListener((e)->{
				try
				{
					tableFormatModel.setNumRows(0);
					tableContentmodel.setNumRows(0);

					Object userObject = ((DefaultMutableTreeNode)e.getPath().getLastPathComponent()).getUserObject();
					
					if (userObject instanceof Table)
					{
						Table table = (Table)userObject;

						frame.setTitle(table.getTableMetadata().toString());

						for (FieldType field : table.getTableMetadata().getFields())
						{
							tableFormatModel.addRow(new Object[]{
								field.getCategory(),
								field.getName(),
								field.getFormat(),
								field.getType().getSimpleName(),
								field.isNullable(),
								Arrays.toString(field.getComponentType()),
								field.getDepth()
							});
						}

						for (Iterator<Entry> it = table.iteratorRaw(); it.hasNext(); )
						{
							Entry entry = it.next();
							tableContentmodel.addRow(new Object[]{new String(entry.getKey()), new String(entry.getValue())});
						}
					}

					tableContent.invalidate();
					tableContent.validate();
					tableContent.repaint();
				}
				catch (Throwable ex)
				{
					ex.printStackTrace(Log.out);
				}
			});

			JSplitPane splitPane = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT);
			splitPane.add(tableListScroll);
			splitPane.add(tabbedPane);

			frame.add(splitPane, BorderLayout.CENTER);
			frame.setSize(1024, 768);
			frame.setLocationRelativeTo(null);
			frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
			frame.addWindowListener(new WindowAdapter()
			{
				@Override
				public void windowClosing(WindowEvent aE)
				{
					try
					{
						database.close();
					}
					catch (Exception e)
					{
						e.printStackTrace(Log.out);
					}
				}
			});
			frame.setVisible(true);
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
	}
}
