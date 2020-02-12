package java;

import javax.swing.*;
import javax.swing.border.EmptyBorder;
import javax.swing.table.AbstractTableModel;
import java.awt.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;

class T1Data extends JPanel {
    private final JTable table;

    public T1Data(String path) {
        super(new BorderLayout(3, 3));

        this.table = new JTable(new MyModel());
        this.table.setPreferredScrollableViewportSize(new Dimension(700, 70));
        this.table.setFillsViewportHeight(true);

        JPanel ButtonOpen = new JPanel(new FlowLayout(FlowLayout.CENTER));
        add(ButtonOpen, BorderLayout.SOUTH);

        JScrollPane scrollPane = new JScrollPane(table);
        add(scrollPane, BorderLayout.CENTER);
        setBorder(new EmptyBorder(5, 5, 5, 5));

        CSVFile Rd = new CSVFile();
        MyModel NewModel = new MyModel();
        this.table.setModel(NewModel);
        File DataFile = new File(path);
        ArrayList<String[]> Rs2 = Rd.ReadCSVfile(DataFile);

        NewModel.AddCSVData(Rs2);
        System.out.println("Rows: " + NewModel.getRowCount());
        System.out.println("Cols: " + NewModel.getColumnCount());
    }


    public class CSVFile {
        private ArrayList<String[]> Rs = new ArrayList<>();
        private String[] OneRow;

        public ArrayList<String[]> ReadCSVfile(File DataFile) {
            try {
                BufferedReader brd = new BufferedReader(
                        new FileReader(DataFile));

                while (brd.readLine() != null) {
                    String st = brd.readLine();
                    OneRow = st.split(",");
                    Rs.add(OneRow);
                }
            }
            catch (Exception e) {
                String errmsg = e.getMessage();
                System.out.println("File not found:" + errmsg);
            }
            return Rs;
        }
    }

    public static void createAndShowGUI(String path) {
        javax.swing.SwingUtilities.invokeLater(new Runnable() {
            public void run() {
                JFrame frame = new JFrame("T1Data");
                frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

                T1Data newContentPane = new T1Data(path);
                frame.setContentPane(newContentPane);

                frame.pack();
                frame.setVisible(true);
            }
        });

    }

    class MyModel extends AbstractTableModel {
        private String[] columnNames = {"id", "date", "total", "val"};
        private ArrayList<String[]> Data = new ArrayList<>();

        public void AddCSVData(ArrayList<String[]> DataIn) {
            this.Data = DataIn;
            this.fireTableDataChanged();
        }

        @Override
        public int getColumnCount() {
            return columnNames.length;
        }

        @Override
        public int getRowCount() {
            return Data.size();
        }

        @Override
        public String getColumnName(int col) {
            return columnNames[col];
        }

        @Override
        public Object getValueAt(int row, int col) {
            return Data.get(row)[col];

        }
    }
}