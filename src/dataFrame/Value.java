package dataFrame;

public abstract class Value {
    protected Object val;
    public Value(Object v){this.val=v;}
    public Value() {}
    public Object getVal(){return this.val;}
    public abstract String toString();
    public abstract Value add(Value val) throws Throwable;
    public abstract Value sub(Value val) throws Throwable;
    public abstract Value mul(Value val);
    public abstract Value div(Value val);
    public abstract Value pow(Value val);
    public abstract boolean eq(Value val);
    public abstract boolean lte(Value val);
    public abstract boolean gte(Value val);
    public abstract boolean neq(Value val);
    public abstract boolean equals(Object other);
    public abstract int hashCode();
    public abstract Value create(String s);
}
