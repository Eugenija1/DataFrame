package java;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DataFrame implements Cloneable{
    protected List<Column> allColumns;

    public static void main(String[] args) throws IOException, CloneNotSupportedException, ClassNotFoundException, NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException, SQLException {
        ArrayList<Class <? extends Value>> types = new ArrayList<>();
        types.add(MyString.class);
        types.add(MyString.class);
        types.add(MyDouble.class);
        types.add(MyDouble.class);
        DataFrame df = new DataFrame("csv_files/groubymulti.csv", types, null);
        df.groupby(new String[]{"id", "date"} ).min().printDF();

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
        String[] sortedColumns;
        GroupedDF(DataFrame df, String[] namesColumns) throws ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException, SQLException{
        sortedColumns = namesColumns;
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

    public GroupedDF() {

    }

    void printGroups(){
        for(DataFrame df : listDF) {
            df.printDF();
        }
    }


    ArrayList<String> getColumnsToSort(DataFrame df){
        String[] all = getNames(df);
        int flag=-1;
        ArrayList<String> notSortedColumns = new ArrayList<>();
        for(String str : all){
            for(int i=0; i<sortedColumns.length; i++) {
                if (str.equals(sortedColumns[i])){
                    flag=-1;
                    break;}
                else {
                    flag = i;
                }
            }
            if(flag>=0)
                notSortedColumns.add(str);
        }
        return notSortedColumns;
    }

            @Override
            public DataFrame max() throws ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException, SQLException {
                DataFrame maxDF = new DataFrame(getNames(listDF.get(0)), getTypes(listDF.get(0)));
                ExecutorService pool = Executors.newFixedThreadPool(2);
                for (int a = 0; a < listDF.size(); a++) {
                    int finalA = a;
                    pool.execute(new Runnable() {
                        @Override
                        public void run() {
                                    Object[] raw = new Object[listDF.get(finalA).numOfColumns()];
                                    int i = 0;
                                    ArrayList<String> notSortedColumns = getColumnsToSort(listDF.get(finalA));
                                    for (int b = 0; b < listDF.get(finalA).numOfColumns(); b++) {
                                        try {
                                            if (!notSortedColumns.contains(listDF.get(finalA).get(b).getNameOfColumn())) {
                                                try {
                                                    raw[b] = listDF.get(finalA).get(b).getElem(0);
                                                } catch (SQLException e) {
                                                    e.printStackTrace();
                                                }
                                            } else {
                                                try {
                                                    raw[b] = Collections.max(listDF.get(finalA).get(notSortedColumns.get(i)).getList());
                                                } catch (ClassNotFoundException e) {
                                                    e.printStackTrace();
                                                }
                                                i++;
                                            }
                                        } catch (SQLException e) {
                                            e.printStackTrace();
                                        }
                                    }
                                    try {
                                        maxDF.addRaw(raw, getTypes(listDF.get(finalA)));

                                    } catch (IllegalAccessException | InstantiationException | NoSuchMethodException | InvocationTargetException e) {
                                        e.printStackTrace();
                                    }
                                }
                    });
                }
                pool.shutdown();
                return maxDF;
            }

        @Override
        public DataFrame min() throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
            DataFrame minDF = new DataFrame(getNames(listDF.get(0)), getTypes(listDF.get(0)));
            Object[] raw = new Object[listDF.get(0).numOfColumns()];
            ArrayList<String> notSortedColumns = getColumnsToSort(listDF.get(0));
            ExecutorService pool = Executors.newFixedThreadPool(2);
            for(DataFrame df : listDF){
                pool.execute(new Runnable() {
                    @Override
                    public void run() {
                        int i = 0;
                        for (int b = 0; b < df.numOfColumns(); b++) {
                            try {
                                if (!notSortedColumns.contains(df.get(b).getNameOfColumn())) {
                                    try {
                                        raw[b] = df.get(b).getElem(0);
                                    } catch (SQLException e) {
                                        e.printStackTrace();
                                    }
                                } else {
                                    raw[b] = Collections.min(df.get(notSortedColumns.get(i)).getList());
                                    i++;
                                }
                            } catch (SQLException | ClassNotFoundException e) {
                                e.printStackTrace();
                            }
                        }
                        try {
                            minDF.addRaw(raw, getTypes(df));
                        } catch (IllegalAccessException | InstantiationException | NoSuchMethodException | InvocationTargetException e) {
                            e.printStackTrace();
                        }
                    }
                });
            }
            pool.shutdown();
            return minDF;
        }

        @Override
        public DataFrame mean() throws ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException, SQLException {
            DataFrame meanDF = new DataFrame(getNames(listDF.get(0)), getTypes(listDF.get(0)));
            Object[] raw = new Object[listDF.get(0).numOfColumns()];
            ArrayList<String> notSortedColumns = getColumnsToSort(listDF.get(0));
            int i=0;
            for(DataFrame df : listDF){
                for(int b =0; b<df.numOfColumns(); b++){
                    if(! notSortedColumns.contains(df.get(b).getNameOfColumn())){
                        raw[b] = df.get(b).getElem(0);
                    }
                    else{
                        MyDouble dbl = (MyDouble) df.get(b).getElem(0);
                        for(int el=1; el<df.get(b).sizeColumn(); el++){
                            dbl.add(((MyDouble) df.get(b).getElem(el)));
                        }
                        raw[b]= dbl.getDouble()/df.get(b).sizeColumn();
                    }
                }
                meanDF.addRaw(raw, getTypes(df));
            }
            return meanDF;
        }

        @Override
        public DataFrame std() {
            return null;
        }

        @Override
        public DataFrame sum() throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException, SQLException {
            DataFrame sumDF = new DataFrame(getNames(listDF.get(0)), getTypes(listDF.get(0)));
            Object[] raw = new Object[listDF.get(0).numOfColumns()];
            ArrayList<String> notSortedColumns = getColumnsToSort(listDF.get(0));
            int i=0;
            for(DataFrame df : listDF){
                for(int b =0; b<df.numOfColumns(); b++){
                    if(! notSortedColumns.contains(df.get(b).getNameOfColumn())){
                        raw[b] = df.get(b).getElem(0);
                    }
                    else{
                        MyDouble dbl = (MyDouble) df.get(b).getElem(0);
                        for(int el=1; el<df.get(b).sizeColumn(); el++){
                            dbl.add(((MyDouble) df.get(b).getElem(el)));
                        }
                        raw[b]= dbl.getDouble();
                    }
                }
                sumDF.addRaw(raw, getTypes(df));
            }
            return sumDF;
        }

        @Override
        public DataFrame var() throws ClassNotFoundException, NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException, SQLException {
            DataFrame varDF = new DataFrame(getNames(listDF.get(0)), getTypes(listDF.get(0)));
            Object[] raw = new Object[listDF.get(0).numOfColumns()];
            ArrayList<String> notSortedColumns = getColumnsToSort(listDF.get(0));
            MyDouble mean, sub, pow, sum;

            for(DataFrame df : listDF){
                for(int b =0; b<df.numOfColumns(); b++){
                    if(! notSortedColumns.contains(df.get(b).getNameOfColumn())){
                        raw[b] = df.get(b).getElem(0);
                    }
                    else{
                        sum= new MyDouble("0");
                        for(int el=0; el<df.get(b).sizeColumn(); el++){
                            sum.add(((MyDouble) df.get(b).getElem(el)));
                        }
                        mean = new MyDouble ( Double.toString(sum.getDouble()/df.get(b).sizeColumn()));
                        sum.create("0");
                        for(int el=0; el<df.get(b).sizeColumn(); el++){
                            sub = (MyDouble) ((MyDouble) df.get(b).getElem(el)).sub(mean);
                            pow = (MyDouble) sub.pow(new MyDouble("2"));
                            sum = (MyDouble) sum.add(pow);
                        }
                        raw[b]= sum.getDouble()/df.get(b).sizeColumn();
                    }
                }
                varDF.addRaw(raw, getTypes(df));
            }
            return varDF;
        }
    }

    public GroupedDF groupby(String[] namesColumns) throws ClassNotFoundException, NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException, SQLException {
        return new GroupedDF(this, namesColumns);
    }


//    public class Comp implements Comparator{
//        @Override
//        public int compare(Object o, Object t1) {
//
//            return 0;
//        }
//    }
    public DataFrame sort() throws SQLException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        HashMap<String, Integer> m1 = new HashMap<>();
        HashMap<String, Integer> m2 = new HashMap<>();
        DataFrame sortedDF = new DataFrame(getNames(this), getTypes(this));
        for (int i=0; i<this.allColumns.get(0).sizeColumn(); i++){
            m1.put((String) this.allColumns.get(0).getElem(i), i);
        }
        List<String> key = new ArrayList<>(m1.keySet());
        Collections.sort(key);
        for(int n=0; n<key.size(); n++){
            m2.put(key.get(n), n);
        }
        Set<Map.Entry<String,Integer>> s = m2.entrySet();
        int el;
        Object[] raw = new Object[this.numOfColumns()];
        for(Map.Entry<String, Integer> it: s){
            el = m1.get(it.getKey());
            raw[0]= it.getValue();
            for(int c=1; c<4; c++){
                raw[c]= this.get(c).getElem(el);
            }
            sortedDF.addRaw(raw, getTypes(this));
        }
        return sortedDF;
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
    public Column get(int i) throws SQLException {
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

    public DataFrame iloc (int i) throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        if(i<0 || i>size()){
            throw new IndexOutOfBoundsException(i + " is out of bound.");
        }
        Object[] record = new String[allColumns.size()];
        for(int n =0; n<allColumns.size(); n++){
                record[n] = allColumns.get(n).getElem(i-1);

        }
        DataFrame df2 = new DataFrame(getNames(this), getTypes(this));
        df2.addRaw(record, getTypes(this));
        return df2;
    }

    public DataFrame iloc(int from, int to) throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        Object[] record = new String[allColumns.size()];
        DataFrame df2 = new DataFrame(getNames(this), getTypes(this));
        for(int ind = from-1; ind<to; ind++) {
            for(int n =0; n<allColumns.size(); n++){
                record[n] = allColumns.get(n).getElem(ind);
            }
            df2.addRaw(record, getTypes(this));
        }
        return df2;
    }

}
