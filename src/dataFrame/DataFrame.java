package dataFrame;

import javax.xml.crypto.Data;
import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.HashMap;
import java.util.Map;

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

    protected class GroupedDF implements Groupby{
        LinkedList<DataFrame> listDF;
        GroupedDF(DataFrame df, String[] colname) throws ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
            HashMap<Integer, HashMap<Object, Integer>> mapOfMaps = new HashMap<>();
            int sizeLinkedList = 1;
            listDF = new LinkedList<DataFrame>();
            for(int iter=0; iter<colname.length; iter++) {
                Stream<Object> str = Arrays.stream(df.get(colname[iter]).getList().toArray());
                List<Object> listStr = str.distinct().collect(Collectors.toList());
                HashMap<Object, Integer> mapStream = new HashMap<>();
                for (int pos = 0; pos < listStr.size(); pos++) {
                    mapStream.put(listStr.get(pos), pos + 1);
                }
                int mapSize = mapStream.size();
                sizeLinkedList = sizeLinkedList * mapSize;
                mapOfMaps.put(iter, mapStream);
            }
                for (int i = 0; i < sizeLinkedList; i++) {
                    listDF.add(new DataFrame(getNames(df), getTypes(df)));
                }

                Object[] raw = new Object[numOfColumns()];
            for (int n = 0; n < df.size(); n++) {
                    for (int wid = 0; wid < df.numOfColumns(); wid++) {
                        raw[wid] = df.get(wid).getElem(n);
                    }
                   int sizeMaps = mapOfMaps.size();
                   double i = 10;
                   double index=0;
                   i = Math.pow(i, sizeMaps-1);
                    for(int iter=0; iter<sizeMaps; iter++){
                        index =index +  (mapOfMaps.get(iter).get(df.get(colname[iter]).getElem(n))-1)*i;
                        i= i/10;
                    }
                    listDF.get((int)index).addRaw(raw, getTypes(df));
                }
            }
        void printGroups(){
            for(DataFrame df : listDF) {
                df.printDF();
            }
        }
    }

    public GroupedDF groupby(String[] colname) throws ClassNotFoundException, NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
        return new GroupedDF(this, colname);
    }


    public static String[] getNames(DataFrame df){
        String[] Names = new String[df.numOfColumns()];

        for (int i=0; i<df.numOfColumns(); i++){
            Names[i] = df.allColumns.get(i).getNameOfColumn();
        }
        return Names;
    }
    public static ArrayList<Class <? extends Value>> getTypes(DataFrame df){
        ArrayList<Class <? extends Value>> Types = new ArrayList<>(df.numOfColumns());

        for (int i=0; i<df.numOfColumns(); i++){
            Types.add(df.allColumns.get(i).getTypeOfColumn());
        }
        return Types;
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
