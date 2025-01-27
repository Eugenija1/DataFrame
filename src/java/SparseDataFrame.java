package java;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.util.ArrayList;

public class SparseDataFrame extends DataFrame {

    private static Object hide;
    protected int size;


    public static void main(String[] args) throws ClassNotFoundException, IOException, NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException, SQLException {
        ArrayList<Class <? extends Value>> types = new ArrayList<>();
        types.add(MyString.class);
        types.add(MyString.class);
        types.add(MyDouble.class);
        types.add(MyDouble.class);
//        SparseDataFrame sdf = new SparseDataFrame(new String[]{"kol1","kol2"}, new String[]{"int","int"}, hide="0");
        DataFrame df = new DataFrame("csv_files/groubymulti.csv", types, null);
//        DataFrame df = new DataFrame(new String[]{"kol1", "kol2", "kol3"}, new String[]{"float", "float", "float"});
//        df.fillColumns();

//        df.printDF();
        df.groupby(new String[]{"id", "date"}).var().printDF();
//        SparseDataFrame sdf = new SparseDataFrame(df, "0");
//        sdf.printDF();
//        sdf.toDense().printDF();
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

    public <cls extends Value> SparseDataFrame(DataFrame df, Object hide_) throws IllegalAccessException, InvocationTargetException, InstantiationException, NoSuchMethodException {
        this(getNames(df), getTypes(df), hide_);
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
        DataFrame df1 = new DataFrame(getNames(this), getTypes(this));
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
