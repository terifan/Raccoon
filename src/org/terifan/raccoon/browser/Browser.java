package org.terifan.raccoon.browser;

import java.awt.BorderLayout;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.File;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
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
import org.terifan.raccoon.io.AccessCredentials;
import org.terifan.raccoon.serialization.FieldType;
import org.terifan.raccoon.util.Log;


public class Browser
{
	public static void main(String ... args)
	{
		try
		{
//			JFileChooser fileChooser = new JFileChooser();
//
//			if (fileChooser.showOpenDialog(null) != JFileChooser.APPROVE_OPTION)
//			{
//				return;
//			}
//
//			String password = JOptionPane.showInputDialog(null, "Password");
//
//			if (password == null || password.isEmpty())
//			{
//				return;
//			}
//			
//			File file = fileChooser.getSelectedFile();
//			AccessCredentials ac = new AccessCredentials(password);

			AccessCredentials ac = new AccessCredentials("test");
			File file = new File("d:/sample.db");

//			try (Database db = Database.open(file, OpenOption.CREATE_NEW))
//			{
//				db.save(new _Fruit1K("apple", 52.12));
//				db.save(new _Fruit1K("orange", 47.78));
//				db.save(new _Fruit1K("banana", 89.45));
//				db.save(new _Number1K2D(1, "yellow", 89, "lemon"));
//				db.save(new _Number1K2D(2, "green", 7, "apple"));
//				db.save(new _Number1K2D(2, "red", 42, "apple"));
//				db.save(new _Number1K2D(1, "yellow", 13, "banan"));
//				db.save(new _Object1K("test", new GregorianCalendar()));
//				db.save(new _Number1K1D("a", 1));
//				db.save(new _Number1K1D("b", 2));
//				db.save(new _Number1K1D("c", 3));
//				db.save(new _Number1K1D("d", 4));
//				db.save(new _BigObject1K().random());
//				db.save(new _BigObject1K().random());
//				db.save(new _BigObject1K().random());
//				db.commit();
//			}

			UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
			
			JFrame frame = new JFrame();

			final Database database = Database.open(file, OpenOption.READ_ONLY, ac);

			final DefaultTableModel tableContentmodel = new DefaultTableModel();
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
							try
							{
								return ((Table)userObject).getTableMetadata().getDiscriminatorDescription();
							}
							catch (Throwable e)
							{
//								Log.out.println(e.getMessage());
								return "";
							}
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
			tableContent.setAutoCreateRowSorter(true);
			JScrollPane tableListScroll = new JScrollPane(tableList);
			JScrollPane tableContentScroll = new JScrollPane(tableContent, JScrollPane.VERTICAL_SCROLLBAR_AS_NEEDED, JScrollPane.HORIZONTAL_SCROLLBAR_AS_NEEDED);
			JScrollPane tableFormatScroll = new JScrollPane(tableFormat);
			JTabbedPane tabbedPane = new JTabbedPane();
			tabbedPane.add("Format", tableFormatScroll);
			tabbedPane.add("Content", tableContentScroll);
			tableContent.setAutoResizeMode(JTable.AUTO_RESIZE_OFF);

			tableList.addTreeSelectionListener((e)->{
				try
				{
					tableFormatModel.setNumRows(0);
					tableContentmodel.setNumRows(0);

					Object userObject = ((DefaultMutableTreeNode)e.getPath().getLastPathComponent()).getUserObject();
					
					if (userObject instanceof Table)
					{
						tableContentmodel.setColumnCount(0);

						Table table = (Table)userObject;
						FieldType[] fields = table.getTableMetadata().getFields();

						for (FieldType field : fields)
						{
							tableContentmodel.addColumn(field.getName());
						}

						frame.setTitle(table.getTableMetadata().toString());

						for (FieldType field : fields)
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

							Map<String, ?> map = table.getTableMetadata().getMarshaller().unmarshal(entry);

							Object[] values = new Object[map.size()];
							for (int i = 0; i < fields.length; i++)
							{
								values[i] = map.get(fields[i].getName()); 
							}

							tableContentmodel.addRow(values);
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
