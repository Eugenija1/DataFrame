package java;

import java.util.Objects;

public class MyFloat extends Value{
    public MyFloat(String f){
        super(f);
        this.val = Float.parseFloat(f);
    }
    public float getFloat(){return (float) this.val;}

    public String toString(){
        return String.valueOf(this.val);
    }
    public Value add(Value val) throws IllegalArgumentException{
        if(val instanceof MyFloat)
            this.val = this.getFloat() + ((MyFloat) val).getFloat();
        else
            throw new IllegalArgumentException("not float type");
        return this;
    }
    public Value sub(Value val){
        this.val = this.getFloat() - ((MyFloat) val).getFloat();
        return this;
    };
    public Value mul(Value val){
        this.val = this.getFloat() * ((MyFloat) val).getFloat();
        return this;
    };
    public Value div(Value val){
        this.val = this.getFloat() / ((MyFloat) val).getFloat();
        return this;
    };
    public Value pow(Value val){
        this.val = Math.pow(this.getFloat(), ((MyFloat) val).getFloat());
        return this;
    };
    public boolean eq(Value val){
        return this.getFloat() == ((MyFloat) val).getFloat();
    };
    public boolean lte(Value val){
        return this.getFloat() < ((MyFloat) val).getFloat();
    };
    public boolean gte(Value val){
        return this.getFloat() > ((MyFloat) val).getFloat();
    };
    public boolean neq(Value val){
        return getFloat() != ((MyFloat) val).getFloat();
    };

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MyFloat aFloat = (MyFloat) o;
        return Float.compare(aFloat.getFloat(), this.getFloat()) == 0;  //0 - numerically equal, > d1>d2 ...
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.getFloat());
    };
    public Value create(java.lang.String s){
        this.val = java.lang.Float.parseFloat(s);
        return this;
    };
}

