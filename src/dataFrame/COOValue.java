package dataFrame;

import java.util.Objects;

public  class COOValue extends Value{
    int position;
    Object element;

    public COOValue(int pos, Object el){
        super();
        this.position = pos;
        this.element = el;
    }
    public int getPosition(){
        return this.position;
    }
    public Object getElement(){
        return this.element;
    }
    public String toString(){
        return "("+ getPosition() +", "+getElement()+")";
    }
    public Value add(Value val){
        if(val instanceof COOValue)
            try {
                int value = (int) element;
                value = value + (int) ((COOValue) val).getElement();
                element=value;
            } catch (java.lang.ClassCastException nfe) {System.out.println("You cannot add these types o objects!");}
        try {
            double value = (double) element;
            value = value + (double) ((COOValue) val).getElement();
            element =value;
        } catch (java.lang.ClassCastException nfe) {System.out.println("You cannot add these types o objects!");}
        return this;
    }
    public Value sub(Value val){
        if(val instanceof COOValue)
            try {
                int value = (int) element;
                value = value - (int) ((COOValue) val).getElement();
                element = value;
            } catch (java.lang.ClassCastException nfe) {System.out.println("You cannot subtract these types o objects!");}
        try {
            double value = (double) element;
            value = value - (double) ((COOValue) val).getElement();
            element = value;
        } catch (java.lang.ClassCastException nfe) {System.out.println("You cannot subtract these types o objects!");}
        return this;
    };
    public Value mul(Value val){
        if(val instanceof COOValue)
            try {
                int value = (int) element;
                value = value * (int) ((COOValue) val).getElement();
                element=value;
            } catch (java.lang.ClassCastException nfe) {System.out.println("You cannot multiply these types o objects!");}
        try {
            double value = (double) element;
            value = value * (double) ((COOValue) val).getElement();
            element=value;
        } catch (java.lang.ClassCastException nfe) {System.out.println("You cannot multiply these types o objects!");}
        return this;
    };
    public Value div(Value val){
        if(val instanceof COOValue)
            try {
                int value = (int) element;
                value = value / (int) ((COOValue) val).getElement();
                element = value;
            } catch (java.lang.ClassCastException nfe) {System.out.println("You cannot divide these types o objects!");}
        try {
            double value = (double) element;
            value = value / (double) ((COOValue) val).getElement();
            element = value;
        } catch (java.lang.ClassCastException nfe) {System.out.println("You cannot divide these types o objects!");}
        return this;
    };
    public Value pow(Value val){
        if(val instanceof COOValue)
            try {
                double value = (double) element;
                value = Math.pow(value, (int) ((COOValue) val).getElement());
                element = value;
            } catch (java.lang.ClassCastException nfe) {System.out.println("You cannot power these types o objects!");}
        return this;
    };
    public boolean eq(Value val){
        if(val instanceof COOValue){
            return position==((COOValue) val).getPosition() && element==((COOValue) val).getElement();
        }else{
            return false;
        }
    };
    public boolean lte(Value val){
        if(val instanceof COOValue)
            try {
                double value = (double) element;
                return value < (double)((COOValue) val).getElement();
            } catch (java.lang.ClassCastException nfe) {
                System.out.println("You cannot compare these types o objects!");
            }
        return false;
    };
    public boolean gte(Value val){
        if(val instanceof COOValue)
            try {
                double value = (double) element;
                return value > (double)((COOValue) val).getElement();
            } catch (java.lang.ClassCastException nfe) {
                System.out.println("You cannot compare these types o objects!");
            }
        return false;
    };
    public boolean neq(Value val){
        if(val instanceof COOValue)
            try {
                double value = (double) element;
                return value != (double)((COOValue) val).getElement();
            } catch (java.lang.ClassCastException nfe) {
                System.out.println("You cannot compare these types o objects!");
            }
        return false;
    };


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        COOValue value = (COOValue) o;
        return position == value.position &&
                Objects.equals(element, value.element);
    }

    @Override
    public int hashCode() {
        return Objects.hash(position, element);
    }

    public Value create(String s){ //TODO
        this.val = java.lang.Double.parseDouble(s);
        return this;
    };
}
