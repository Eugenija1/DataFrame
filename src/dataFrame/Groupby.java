package dataFrame;

import java.lang.reflect.InvocationTargetException;

public interface Groupby {
    DataFrame max() throws ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException;
    DataFrame min();
    DataFrame mean();
    DataFrame std();
    DataFrame sum();
    DataFrame var();
//    DataFrame apply(Applayable);
}
