package dataFrame;

public class MyString extends Value{
    public MyString(MyString d){
        super(d);
    }
    public String getString(){return (String) this.val;}

    public String toString(){
        return this.getString();
    }
    public Value add(Value val) throws Throwable {
        if(val instanceof MyString)
            this.val = this.getString() + ((MyString) val).getString();
        else
            throw new IllegalArgumentException("values are not strings");
        return this;
    }
    public boolean eq(Value val){
        return this.getString().equals(((MyString) val).getString());
    };
    public boolean lte(Value val){
        return this.getString().length() < ((MyString) val).getString().length();
    };
    public boolean gte(Value val){
        return this.getString().length() > ((MyString) val).getString().length();
    };
    public boolean neq(Value val){
        return !getString().equals(((MyString) val).getString());
    };
    public Value sub(Value val) throws IllegalArgumentException{
        throw new IllegalArgumentException("You cannot subtract stings");
    };
    public Value mul(Value val){
        throw new IllegalArgumentException("You cannot multiply stings");
    };
    public Value div(Value val){
        throw new IllegalArgumentException("You cannot divide stings");
    };
    public Value pow(Value val){
        throw new IllegalArgumentException("You cannot power stings");
    };
    public Value create(String s){
        this.val = s;
        return this;
    };
    public boolean equals(Object other){
        if (this == other) return true; //sprawdza referencje, czy wskazuja na to samo miejsce w pamieci
        if (other == null || getClass() != other.getClass()) return false; //spr czy istnieje i czy objekty sa tej samej klasy
        MyString aString = (MyString) other; //klasy sa rowne -> rzutujemy object na mydouble
        return this.getString().equals(aString.getString()); //0 - numerically equal, > d1>d2 ...
    };
    public int hashCode(){
        return this.getString().hashCode();
    };
}

