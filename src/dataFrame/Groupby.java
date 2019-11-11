package dataFrame;

import java.lang.reflect.InvocationTargetException;

public interface Groupby {
    DataFrame max() throws ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException;
    DataFrame min() throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException, ClassNotFoundException;
    DataFrame mean() throws ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException;
    DataFrame std();
    DataFrame sum() throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException;
    DataFrame var();
//    DataFrame apply(Applayable);
}
