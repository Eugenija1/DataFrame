package java;

import java.util.Objects;

public class MyDouble extends Value implements Comparable {
    public MyDouble(){super();}
    public MyDouble(String d){
        super(d);
        this.val = Double.parseDouble(d);
    }
    public double getDouble(){return (double) this.val;}


    public String toString(){
        return String.valueOf(this.val);
    }
    public Value add(Value val) throws IllegalArgumentException {
        if(val instanceof MyDouble)
            this.val = this.getDouble() + ((MyDouble) val).getDouble();
        else
            throw new IllegalArgumentException("object doesn't have value of MyDouble type");
        return this;
    }
    public Value sub(Value val){
        try {
            MyDouble dbVal = (MyDouble) val;
            this.val = this.getDouble() - dbVal.getDouble();
        } catch (java.lang.ClassCastException nfe) {System.out.println("You cannot subtract these types o objects!");}
        return this;
    };
    public Value mul(Value val){
        if(val instanceof MyDouble)
            this.val = this.getDouble() * ((MyDouble) val).getDouble();
        else
            throw new IllegalArgumentException("object doesn't have value of MyDouble type");
        return this;
    };
    public Value div(Value val){
        if(val instanceof MyDouble)
            this.val = this.getDouble() / ((MyDouble) val).getDouble();
        else
            throw new IllegalArgumentException("object doesn't have value of MyDouble type");
        return this;
    };
    public Value pow(Value val){
        if(val instanceof MyDouble)
            this.val = Math.pow(this.getDouble(), ((MyDouble) val).getDouble());
        else
            throw new IllegalArgumentException("object doesn't have value of MyDouble type");
        return this;
    };
    public boolean eq(Value val){//identity - references to the same objects ==
        if(val instanceof MyDouble)
            return this.getDouble() == ((MyDouble) val).getDouble();
        else
            throw new IllegalArgumentException("object doesn't have value of MyDouble type");
    };
    public boolean lte(Value val){
        if(val instanceof MyDouble)
            return this.getDouble() < ((MyDouble) val).getDouble();
        else
            throw new IllegalArgumentException("object doesn't have value of MyDouble type");
    };
    public boolean gte(Value val){
        if(val instanceof MyDouble)
            return this.getDouble() > ((MyDouble) val).getDouble();
        else
            throw new IllegalArgumentException("object doesn't have value of MyDouble type");
    };
    public boolean neq(Value val){
        if(val instanceof MyDouble)
            return getDouble() != ((MyDouble) val).getDouble();
        else
            throw new IllegalArgumentException("object doesn't have value of MyDouble type");
    };

    @Override
    public boolean equals(Object o) { // if referenced values are the same
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MyDouble aDouble = (MyDouble) o;
        return Double.compare(aDouble.getDouble(), this.getDouble()) == 0;  //0 - numerically equal, > d1>d2 ...
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.getDouble());
    };
    public Value create(String s){
        this.val = Double.parseDouble(s);
        return this;
    };

    @Override
    public int compareTo(Object o) {
        return Double.compare(this.getDouble(), ((MyDouble) o).getDouble());
    }
}
