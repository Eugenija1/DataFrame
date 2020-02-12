package java;

import java.util.Objects;

public class MyInteger extends Value{
    public MyInteger(String i){
        super(i);
        this.val = Integer.parseInt(i);
    }
    public int getInteger(){return (int) this.val;}

    public String toString(){
        return String.valueOf(this.val);
    }
    public Value add(Value val) throws IllegalArgumentException {
        if(val instanceof MyInteger)
            this.val = this.getInteger() + ((MyInteger) val).getInteger();
        else
            throw new IllegalArgumentException("not integer type");
        return this;
    }
    public Value sub(Value val){
        this.val = this.getInteger() - ((MyInteger) val).getInteger();
        return this;
    };
    public Value mul(Value val){
        this.val = this.getInteger() * ((MyInteger) val).getInteger();
        return this;
    };
    public Value div(Value val){
        this.val = this.getInteger() / ((MyInteger) val).getInteger();
        return this;
    };
    public Value pow(Value val){
        this.val = Math.pow(this.getInteger(), ((MyInteger) val).getInteger());
        return this;
    };
    public boolean eq(Value val){
        return this.getInteger() == ((MyInteger) val).getInteger();
    };
    public boolean lte(Value val){
        return this.getInteger() < ((MyInteger) val).getInteger();
    };
    public boolean gte(Value val){
        return this.getInteger() > ((MyInteger) val).getInteger();
    };
    public boolean neq(Value val){
        return getInteger() != ((MyInteger) val).getInteger();
    };

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MyInteger aInteger = (MyInteger) o;
        return aInteger.getInteger() == this.getInteger();  //0 - numerically equal, > d1>d2 ...
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.getInteger());
    };
    public Value create(String s){
        this.val = Integer.parseInt(s);
        return this;
    };
}

