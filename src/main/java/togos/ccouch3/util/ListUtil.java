package togos.ccouch3.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ListUtil {
	public static <T> List<T> snoc(List<T> list, T item) {
		ArrayList<T> newList = new ArrayList<T>(list.size()+1);
		newList.addAll(list);
		newList.add(item);
		return Collections.unmodifiableList(newList);
	}
	public static <T> List<T> cdr(List<T> list) {
		return list.subList(1, list.size());
	}
	public static <T> T car(List<T> list) {
		return list.get(0);
	}
}
