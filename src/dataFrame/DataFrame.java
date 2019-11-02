package dataFrame;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public class DataFrame implements Cloneable{
    protected List<Column> allColumns;

    public static void main(String[] args) throws IOException, CloneNotSupportedException, ClassNotFoundException, NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
        ArrayList<Class <? extends Value>> types = new ArrayList<>();
        types.add(MyDouble.class);
        types.add(MyDouble.class);
        types.add(MyDouble.class);
        DataFrame df = new DataFrame("C:\\Users\\Berezka\\IdeaProjects\\L1ZD\\src\\data.csv",types, null);
//       // df.fillColumns();
        System.out.println(df.allColumns.get(0).getNameOfColumn());
        System.out.println(df.allColumns.get(1).getNameOfColumn());
        System.out.println(df.allColumns.get(2).getNameOfColumn());
        df.get(new String[]{"x", "last"}, true);
//        df.printDF();
//        System.out.println("Kolumna 1: ");
//        df.get("last").printColumn();
//        System.out.println("Zwroci rzad o numerze: ");
//        df.iloc(1).printDF();
//        System.out.println("Zwroci rzady w zakresie: ");
//        df.iloc(1, 2).printDF();
//        System.out.println(df.size());
//        System.out.println("nową DF z kolumnami, kopia shallow ");
//        df.printDF(df.get(new String[]{"kol2","kol3"}, true));
//        System.out.println("nową DF z kolumnami, kopia deep ");
//        df.printDF(df.get(new String[]{"kol2","kol3"}, false));
    }
    public DataFrame(){allColumns = new ArrayList<Column>();}

    public DataFrame(String[] nameCol, ArrayList<Class <? extends Value>> types) {
        if (nameCol.length != types.size()) {
            throw new IllegalArgumentException("Number of columnNames and columnTypes are not equal.");
        }
        allColumns = new ArrayList<Column>();
        int i = 0;
        for (Class cl : types) {
            allColumns.add(new Column<>(nameCol[i], cl));
            ++i;
        }
    }

//    public void setAllColumns(List<Column> allColumns) {
//        this.allColumns = allColumns;
//    }
//
//    public void fillColumns() {
//        this.allColumns.get(0).addToColumn(0.0);
//        this.allColumns.get(0).addToColumn(0.0);
//        this.allColumns.get(0).addToColumn(5.0);
//        this.allColumns.get(0).addToColumn(4.0);
//        this.allColumns.get(0).addToColumn(0.0);
//        this.allColumns.get(0).addToColumn(0.0);
//        this.allColumns.get(0).addToColumn(0.0);
//        this.allColumns.get(0).addToColumn(0.0);
//        this.allColumns.get(0).addToColumn(0.0);
//        this.allColumns.get(0).addToColumn(0.0);
//        this.allColumns.get(0).addToColumn(2.0);
//        this.allColumns.get(0).addToColumn(0.0);
//        this.allColumns.get(0).addToColumn(0.0);
//
//        this.allColumns.get(1).addToColumn(0.0);
//        this.allColumns.get(1).addToColumn(0.0);
//        this.allColumns.get(1).addToColumn(7.0);
//        this.allColumns.get(1).addToColumn(0.0);
//        this.allColumns.get(1).addToColumn(0.0);
//        this.allColumns.get(1).addToColumn(3.0);
//        this.allColumns.get(1).addToColumn(0.0);
//        this.allColumns.get(1).addToColumn(0.0);
//        this.allColumns.get(1).addToColumn(0.0);
//        this.allColumns.get(1).addToColumn(0.0);
//        this.allColumns.get(1).addToColumn(0.0);
//        this.allColumns.get(1).addToColumn(0.0);
//        this.allColumns.get(1).addToColumn(0.0);
//
//        this.allColumns.get(2).addToColumn(0.0);
//        this.allColumns.get(2).addToColumn(9.0);
//        this.allColumns.get(2).addToColumn(3.0);
//        this.allColumns.get(2).addToColumn(0.0);
//        this.allColumns.get(2).addToColumn(0.0);
//        this.allColumns.get(2).addToColumn(0.0);
//        this.allColumns.get(2).addToColumn(0.0);
//        this.allColumns.get(2).addToColumn(0.0);
//        this.allColumns.get(2).addToColumn(0.0);
//        this.allColumns.get(2).addToColumn(0.0);
//        this.allColumns.get(2).addToColumn(0.0);
//        this.allColumns.get(2).addToColumn(0.0);
//        this.allColumns.get(2).addToColumn(0.0);
//
//    }

    public <cls> void addRaw(Object[] args, ArrayList<Class <? extends Value>> columnTypes) throws IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
        if(args.length > allColumns.size()){
            throw new IllegalArgumentException("Too many arguments");
        }
        else if(args.length < allColumns.size()){
            throw new IllegalArgumentException("Too little arguments");
        }
        Object v;
        for (int i=0; i<args.length; i++){
            Class cls = columnTypes.get(i);
            Constructor<?> ctr = cls.getConstructor(String.class);
            v = ctr.newInstance(args[i].toString());

            allColumns.get(i).addToColumn((cls) v);

            //cls d = (cls) v;

//            ((Value) v).create(args[i].toString());
        }
    }

    public DataFrame(String fileName, ArrayList<Class <? extends Value>> columnTypes, String[] columnNames) throws IOException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        FileInputStream fileStream = new FileInputStream(fileName);
        BufferedReader br = new BufferedReader(new InputStreamReader(fileStream));

        if(columnNames != null){
            allColumns = new ArrayList<>();
            for (int i=0; i<columnNames.length; i++) {
                allColumns.add(new Column(columnNames[i], columnTypes.get(i)));
            }
        }

        String strLine;
        String splited;
        String[] splitedArray;

        if(columnNames == null){
            strLine = br.readLine();
            splited = strLine;
            splitedArray = splited.split(",");

            allColumns = new ArrayList<>();
            for (int i=0; i<splitedArray.length; i++) {
                allColumns.add(new Column(splitedArray[i], columnTypes.get(i)));
            }
        }

        while ((strLine = br.readLine()) != null)  {
            splited = strLine;
            splitedArray = splited.split(",");
            addRaw(splitedArray, columnTypes);
        }
        br.close();
    }

    public void printDF() {
        for (Column col : allColumns) {
            System.out.printf("%35s", col.getNameOfColumn());
        }
        System.out.println();
//        for(Column col : allColumns){
//            for(int i=0; i<col.sizeColumn(); i++){
//                System.out.printf("%25s", col.getElem(i));
//            }

        for (int i = 0; i < size(); i++) {
            for (Column col : allColumns) {
                try {
                    COOValue value = (COOValue) col.getList().get(i);
                    System.out.printf("%35s", value.toString());
                } catch (java.lang.ClassCastException nfe) {System.out.printf("%35s", col.getList().get(i));}
                catch (java.lang.IndexOutOfBoundsException ind){System.out.printf("%35s", " ");}
            }
            System.out.println();
        }
        System.out.println();
    }


    public int size(){
        return allColumns.get(0).sizeColumn();
    }
    public int numOfColumns(){
        return this.allColumns.size();
    }
    public Column get(int i){
        return this.allColumns.get(i);
    }
    public Column get(String colname) throws ClassNotFoundException {
        for(Column innerList: allColumns) {
            if (innerList.getNameOfColumn().equals(colname))
                return innerList;
            else{
                throw new ClassNotFoundException("There's no such column!");
            }
        }
        throw new ClassNotFoundException("There're no such columns!");
    }

    private DataFrame(ArrayList<Column> columns) {
        this.allColumns=columns;
    }

    public void get(String[] cols, boolean copy) throws CloneNotSupportedException{
//        ArrayList<Column> chosenColumns = new ArrayList<>();
//        for(String colName : cols)
//        {
//            chosenColumns.add(get(colName));
//        }
//        for(Column col : chosenColumns){
//            col.printColumn();
//        }
//        //TODO bug - always deep copy
//        if(copy){
//            DataFrame newDF = new DataFrame(chosenColumns);
//            return (DataFrame) newDF.clone();
//        }
//        else {
//            return new DataFrame(chosenColumns);
//        }
        for (int i = 0; i < size(); i++) {
            for (int n = 0; n < numOfColumns(); n++) {

            }
        }
    }

//    public DataFrame iloc (int i){
//        if(i<0 || i>size()){
//            throw new IndexOutOfBoundsException(i + " is out of bound.");
//        }
//        String[] namecols = new String[allColumns.size()];
//        String[] typecols = new String[allColumns.size()];
//        String[] record = new String[allColumns.size()];
//        for(int n =0; n<allColumns.size(); n++){
//                namecols[n] = allColumns.get(n).getNameOfColumn();
//                typecols[n] = allColumns.get(n).getTypeOfColumn();
//                record[n] = (String) allColumns.get(n).getElem(i-1);
//
//        }
//        DataFrame df2 = new DataFrame(namecols, typecols);
//        df2.addRaw(record);
//        return df2;
//    }
//
//    public DataFrame iloc(int from, int to){
//        String[] namecols = new String[allColumns.size()];
//        String[] typecols = new String[allColumns.size()];
//        String[] record = new String[allColumns.size()];
//        for(int n =0; n<allColumns.size(); n++){
//            namecols[n] = allColumns.get(n).getNameOfColumn();
//            typecols[n] = allColumns.get(n).getTypeOfColumn();
//        }
//        DataFrame df2 = new DataFrame(namecols, typecols);
//        for(int ind = from-1; ind<to; ind++) {
//            for(int n =0; n<allColumns.size(); n++){
//                record[n] = (String) allColumns.get(n).getElem(ind);
//            }
//            df2.addRaw(record);
//        }
//        return df2;
//    }

}
