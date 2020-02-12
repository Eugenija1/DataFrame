package java;

import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;

public interface Groupby{
    DataFrame max() throws ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException, SQLException;
    DataFrame min() throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException;
    DataFrame mean() throws ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException, SQLException;
    DataFrame std();
    DataFrame sum() throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException, SQLException;
    DataFrame var() throws ClassNotFoundException, NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException, SQLException;
}
