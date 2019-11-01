package dataFrame;

import java.io.IOException;

public class SparseDataFrame extends DataFrame {

    private static Object hide;
    protected int size;


    public static void main(String[] args) throws ClassNotFoundException, IOException {
//        SparseDataFrame sdf = new SparseDataFrame(new String[]{"kol1","kol2"}, new String[]{"int","int"}, hide="0");
        DataFrame df = new DataFrame("C:\\Users\\Berezka\\IdeaProjects\\L1ZD\\src\\sparse.csv", new String[]{"float", "float", "float"}, null);
//        DataFrame df = new DataFrame(new String[]{"kol1", "kol2", "kol3"}, new String[]{"float", "float", "float"});
//        df.fillColumns();

        df.printDF();
        SparseDataFrame sdf = new SparseDataFrame(df, "0");
        sdf.printDF();
        sdf.toDense().printDF();
    }

    public SparseDataFrame(String[] names, String[] types, Object hide_){
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
    public static String[] getSpTypes(DataFrame df){
        String[] sparseTypes = new String[df.numOfColumns()];

        for (int i=0; i<df.numOfColumns(); i++){
            sparseTypes[i] = df.allColumns.get(i).getTypeOfColumn();
        }
        return sparseTypes;
    }

    public SparseDataFrame(DataFrame df, Object hide_) {
        this(getSpNames(df), getSpTypes(df), hide_);

        for (int i = 0; i < df.size(); i++) {
            for (int n = 0; n < numOfColumns(); n++) {
                double d = java.lang.Double.parseDouble((String) df.allColumns.get(n).getElem(i));
                double c = java.lang.Double.parseDouble((String) GetHide());
                if (d != c){
                    allColumns.get(n).addToColumn(new COOValue(i, df.allColumns.get(n).getElem(i)));
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

    public boolean checkTypes(String[] types){
        boolean flag = true;
        String first = types[0];
        for(int i = 1; i < types.length && flag; i++) {
            if (!types[i].equals(first)) {
                flag = false;
                return flag;
            }
        }
        return true;
    }


}
