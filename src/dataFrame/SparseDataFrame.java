package dataFrame;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

public class SparseDataFrame extends DataFrame {

    private static Object hide;
    protected int size;


    public static void main(String[] args) throws ClassNotFoundException, IOException, NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
        ArrayList<Class <? extends Value>> types = new ArrayList<>();
        types.add(MyDouble.class);
        types.add(MyDouble.class);
        types.add(MyDouble.class);
//        SparseDataFrame sdf = new SparseDataFrame(new String[]{"kol1","kol2"}, new String[]{"int","int"}, hide="0");
        DataFrame df = new DataFrame("C:\\Users\\Berezka\\IdeaProjects\\L1ZD\\src\\sparse.csv", types, null);
//        DataFrame df = new DataFrame(new String[]{"kol1", "kol2", "kol3"}, new String[]{"float", "float", "float"});
//        df.fillColumns();

//        df.printDF();
        SparseDataFrame sdf = new SparseDataFrame(df, "0");
        sdf.printDF();
        sdf.toDense().printDF();
    }

    public SparseDataFrame(String[] names, ArrayList<Class <? extends Value>> types, Object hide_){
        super(names, types);
        if(!checkTypes(types))
            throw new IllegalArgumentException("Types aren't equal");
        hide=hide_;
        this.size = this.size();
    }
    public Object GetHide(){
        return hide;
    }
    public static String[] getSpNames(DataFrame df){
        String[] sparseNames = new String[df.numOfColumns()];

        for (int i=0; i<df.numOfColumns(); i++){
            sparseNames[i] = df.allColumns.get(i).getNameOfColumn();
        }
        return sparseNames;
    }
    public static ArrayList<Class <? extends Value>> getSpTypes(DataFrame df){
        ArrayList<Class <? extends Value>> sparseTypes = new ArrayList<>(df.numOfColumns());

        for (int i=0; i<df.numOfColumns(); i++){
            sparseTypes.add(df.allColumns.get(i).getTypeOfColumn());
        }
        return sparseTypes;
    }

    public <cls extends Value> SparseDataFrame(DataFrame df, Object hide_) throws IllegalAccessException, InvocationTargetException, InstantiationException, NoSuchMethodException {
        this(getSpNames(df), getSpTypes(df), hide_);
        Class cls = df.allColumns.get(0).getElem(0).getClass();
        Constructor<?> ctr = cls.getConstructor(String.class);
        cls val = (cls) ctr.newInstance((String) GetHide());
        System.out.println(val);
        for (int i = 0; i < df.size(); i++) {
            for (int n = 0; n < numOfColumns(); n++) {
                cls elem = (cls) df.allColumns.get(n).getElem(i);
                if (!elem.equals(val)){
                    allColumns.get(n).addToColumn(new COOValue(i, elem.add(new MyDouble("100000"))));
                }
            }
        }
        this.size = df.size(); //rozmiar pierwotnej daty frame
    }

    public DataFrame toDense(){
        DataFrame df1 = new DataFrame(getSpNames(this), getSpTypes(this));
        int n=0;
        int cp;
        COOValue cv;
        for (Column col : allColumns) {
            cp=0;
            for(int i=0; i<col.sizeColumn(); i++){
                cv = (COOValue) col.getElem(i);
                while(cv.getPosition()!=cp) {
                    df1.allColumns.get(n).addToColumn(hide);
                    cp++;
                }
                df1.allColumns.get(n).addToColumn(cv.getElement());
                cp++;
            }
            if(cp!=size){
                for(int d=0; d<(size-cp); d++){
                    df1.allColumns.get(n).addToColumn(hide);
                }
            }
            n++;
        }
        return df1;
    }

    public boolean checkTypes(ArrayList<Class <? extends Value>> types){
        boolean flag = true;
        Class first = types.get(0);
        for(int i = 1; i < types.size() && flag; i++) {
            if (!types.get(i).equals(first)) {
                flag = false;
                return flag;
            }
        }
        return true;
    }


}
