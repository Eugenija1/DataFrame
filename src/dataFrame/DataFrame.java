package dataFrame;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

        String strLine, splited;
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
        GroupedDF(DataFrame df, String[] namesColumns) throws ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {

            HashMap<Integer, HashMap<Object, Integer>> mapOfMaps = new HashMap<>(); //mapa w ktorej sa przefiltrowane mapy bez powtorzen,
            //klucz - element DF, wartosc - wskazuje do ktorej wyjsciowej DF przekierowac element

            int sizeLinkedList = 1;
            listDF = new LinkedList<DataFrame>(); //lista z wyjsciowymi DF
            for(int iter=0; iter<namesColumns.length; iter++) {
                AtomicInteger i = new AtomicInteger(-1);
                Stream<Object> str = Arrays.stream(df.get(namesColumns[iter]).getList().toArray());// z kolumny tworzymy stream
                HashMap<Object, Integer> mapStream = str.distinct().collect(Collectors.toMap(p->p, p-> i.incrementAndGet(), (s1, s2) -> s2, HashMap::new));
                //zostawiamy tylko unikalne elementy, tworzymy ze streama liste

                sizeLinkedList = sizeLinkedList * mapStream.size();
                mapOfMaps.put(iter, mapStream);
            }

                for (int id = 0; id < sizeLinkedList; id++) {
                    listDF.add(new DataFrame(getNames(df), getTypes(df)));
                }

            Object[] raw = new Object[numOfColumns()];
            List<Integer> mapSizes = new ArrayList<>();
            mapOfMaps.forEach((k, v)-> mapSizes.add(v.size()));
            mapSizes.add(1);
            int index;
            for (int n = 0; n < df.size(); n++) {
                    for (int wid = 0; wid < df.numOfColumns(); wid++) {
                        raw[wid] = df.get(wid).getElem(n);
                    }
                   index=0;
                    for(int iter=0; iter<mapOfMaps.size(); iter++){
                        index += (mapOfMaps.get(iter).get(df.get(namesColumns[iter]).getElem(n)))*mapSizes.get(iter+1);
                    }
                    listDF.get(index).addRaw(raw, getTypes(df));
                }
            }
        void printGroups(){
            for(DataFrame df : listDF) {
                df.printDF();
            }
        }

        @Override
        public DataFrame max() {
            return null;
        }

        @Override
        public DataFrame min() {
            return null;
        }

        @Override
        public DataFrame mean() {
            return null;
        }

        @Override
        public DataFrame std() {
            return null;
        }

        @Override
        public DataFrame sum() {
            return null;
        }

        @Override
        public DataFrame var() {
            return null;
        }

        @Override
        public DataFrame apply() {
            return null;
        }
    }

    public GroupedDF groupby(String[] namesColumns) throws ClassNotFoundException, NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
        return new GroupedDF(this, namesColumns);
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
