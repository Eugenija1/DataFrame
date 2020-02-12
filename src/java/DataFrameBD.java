package java;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.ArrayList;


public class DataFrameBD extends DataFrame {
    String[] names;
    ArrayList<Class <? extends Value>> typesCol;
    Connection c = null;
    Statement stmt = null;

    DataFrameBD(){}

    DataFrameBD(String filename, ArrayList<Class <? extends Value>> columnTypes) throws SQLException {
        super();
        typesCol = columnTypes;
        Connect();
        createTable(filename);
    }

    public void Connect() {
        try {
            Class.forName("org.sqlite.JDBC");
            c = DriverManager.getConnection("jdbc:sqlite:C:\\sqlite\\nowa.db");
            System.out.println("Connected to DB");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public void createTable(String filename) throws SQLException {
        try {
            FileInputStream fileStream = new FileInputStream(filename);
            BufferedReader br = new BufferedReader(new InputStreamReader(fileStream));

            String strLine, splited;
            String[] splitedArray;

            strLine = br.readLine();
            splited = strLine;
            splitedArray = splited.split(",");
            names = splitedArray;

            Statement stmt = c.createStatement();
            String sql = "CREATE TABLE IF NOT EXISTS tabela (\n"
                    //+ "    keys INTEGER PRIMARY KEY AUTOINCREMENT,\n"
                    + "    id char,\n"
                    + "    date text NOT NULL,\n"
                    + "    total real,\n"
                    + "    val real\n"
                    + ");";
            stmt.execute(sql);
            System.out.println("executed");
            while ((strLine = br.readLine()) != null)  {
                splited = strLine;
                splitedArray = splited.split(",");
                insert(splitedArray, names);
            }
            br.close();
            printTable(names);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void insert(String[] splitedArray, String[] names) {
        String sql = "INSERT INTO tabela("+names[0]+", "+names[1]+", "+names[2]+", "+names[3]+") VALUES(?,?,?,?)";

        try {
            PreparedStatement pstmt = c.prepareStatement(sql);
            pstmt.setString(1, splitedArray[0]);
            pstmt.setString(2, splitedArray[1]);
            pstmt.setDouble(3, Double.parseDouble(splitedArray[2]));
            pstmt.setDouble(4, Double.parseDouble(splitedArray[3]));
            pstmt.executeUpdate();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public DataFrame toDF(){
        String sql = "SELECT id, date, total, val FROM tabela";
        DataFrame df2= new DataFrame(names, typesCol);

        try (
                Statement stmt  = c.createStatement();
                ResultSet rs    = stmt.executeQuery(sql)){
            // loop through the result set
            Object[] v = new Object[df2.numOfColumns()];
            while (rs.next()) {
                for(int i =0; i<names.length; i++){
                    v[i] = rs.getObject(names[i]);
                }
                df2.addRaw(v, typesCol);
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
            e.printStackTrace();
        }
        return df2;
    }

    public void printTable(String[] names) {
        try {
            stmt = c.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT * FROM tabela;");
            for (int i=0; i<names.length; i++){System.out.print(names[i]+"\t");}
            System.out.println();
            while (rs.next()) {
                System.out.println(rs.getString(names[0])+  "\t" +
                        rs.getString(names[1]) + "\t" +
                        rs.getDouble(names[2]) + "\t" +
                        rs.getDouble(names[3]));
            }
            rs.close();
            stmt.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    @Override
    public GroupedDFDB groupby(String[] namesColumns){
        GroupedDFDB dfdb = new GroupedDFDB();
        for(int i=0; i<names.length;i++){
            for (String namesColumn : namesColumns) {
                if (names[i].equals(namesColumn))
                    dfdb.groupCols.add(i+1);
            }
        }
        return dfdb;
    }


    protected class GroupedDFDB extends GroupedDF{
        ArrayList<Integer> groupCols;

        GroupedDFDB(){groupCols = new ArrayList<>();}
        @Override
        public DataFrameBD max(){
            try {
                ResultSet rs = stmt.executeQuery("SELECT * FROM tabela;");
//                ResultSetMetaData metadata = rs.getMetaData();
//                String columnName = metadata.getColumnName(groupCols.get(0));
//                System.out.println(columnName);

                String sql = "SELECT id, MAX(total) AS max_total, MAX(val) AS max_val FROM tabela " +
                        "GROUP BY id;";
//                PreparedStatement pstmt = c.prepareStatement(sql);
//                pstmt.setString(1, columnName);
//                pstmt.setString(2, columnName);
//                ResultSet rs2 = pstmt.executeQuery();
                ResultSet rs2 = stmt.executeQuery(sql);
                while (rs2.next()) {
                    System.out.println(rs2.getString("id")+  "\t" +
                            rs2.getDouble("max_total") + "\t" +
                            rs2.getDouble("max_val"));
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
            return new DataFrameBD();
        }
    }

    @Override
    public Column get(int i) throws SQLException {
        try {
            ResultSet rs = stmt.executeQuery("SELECT * FROM tabela;");
            ResultSetMetaData metadata = rs.getMetaData();
            String columnName = metadata.getColumnName(i);

            while (rs.next()) {
                System.out.println(rs.getString(columnName));

            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return new Column();
    }

    @Override
    public DataFrame iloc (int i) throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        try {

            Statement stmt = c.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT * FROM tabela;");
            int num=1;
            while(num<=i){
                rs.next();
                num++;
            }
            System.out.println("\nSelected raw: ");
            System.out.println(rs.getString(names[0])+  "\t" +
                    rs.getString(names[1]) + "\t" +
                    rs.getDouble(names[2]) + "\t" +
                    rs.getDouble(names[3]));
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return new DataFrame();
    }

    @Override
    public DataFrame iloc (int from, int to) throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        try {

            Statement stmt = c.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT * FROM tabela;");
            int num=1;
            while(num<=from){
                rs.next();
                num++;
            }
            System.out.println("\nSelected raws: ");
            while (num<=to){
                System.out.println(rs.getString(names[0])+  "\t" +
                        rs.getString(names[1]) + "\t" +
                        rs.getDouble(names[2]) + "\t" +
                        rs.getDouble(names[3]));
                rs.next();
                num++;
            }

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return new DataFrame();
    }

    public void close(){
        try{
            c.close();
            System.out.println("Closed");
        }catch(Exception e){
            System.out.println(e.getMessage());
        }
    }


    public static void main(String[] args) throws SQLException, NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
        ArrayList<Class <? extends Value>> types = new ArrayList<>();
        types.add(MyString.class);
        types.add(MyString.class);
        types.add(MyDouble.class);
        types.add(MyDouble.class);
        DataFrameBD db = new DataFrameBD("csv_files/groubymulti.csv", types);
        db.iloc(2, 4);
        db.groupby(new String[]{"id"}).max();

        db.close();
    }
}