package dataFrame;

import java.util.ArrayList;

public class Column<Value> implements Cloneable{
    private String nameCol;
    private Class type;
    private ArrayList<Value> arr;

    Column(String nameColumn, Class type_){
        this.nameCol = nameColumn;
        this.type = type_;
//        switch(type){
//            case "double":
//                arr = (ArrayList<Value>) new ArrayList<Double>();
//        }
        arr = new ArrayList<>();
    }

    String getNameOfColumn(){
        return this.nameCol;
    }
    Class getTypeOfColumn(){
        return this.type;
    }
    ArrayList<Value> getList(){return this.arr;}

    void addToColumn(Value obj){
        this.arr.add(obj);
    }
    void addAllToColumn(ArrayList oldList){
        arr.addAll(oldList);
    }
    public void setNameOfColumn(String name){
        this.nameCol = name;
    }
    public void setTypeOfColumn(Class type){ this.type = type; }
    int sizeColumn(){
        return this.arr.size();
    }
    Value getElem(int i){
        return this.arr.get(i);
    }
    public Object cloneS() throws
            CloneNotSupportedException
    {
        return super.clone();
    }
    public void printColumn() {
        System.out.printf("%25s\n", getNameOfColumn());
        for (int i = 0; i < sizeColumn(); i++) {
            System.out.printf("%25s\n", getList().get(i));
        }
    }
}
