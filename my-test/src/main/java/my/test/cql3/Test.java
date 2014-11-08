package my.test.cql3;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Test {

    static class A {
        
    }
    /**
     * @param args
     */
    public static void main(String[] args) {
        // Sort the sstables by hotness (coldest-first). We first build a map because the hotness may change during the sort.
        final Map<String, Double> hotnessSnapshot = new HashMap<String, Double>();
        
        hotnessSnapshot.put("a", 2.0);
        hotnessSnapshot.put("b", 3.0);
        hotnessSnapshot.put("c", 1.0);
        
        List<String> sstables = new ArrayList<String>();
        sstables.addAll(hotnessSnapshot.keySet());
        Collections.sort(sstables, new Comparator<String>()
        {
            public int compare(String o1, String o2)
            {
                int comparison = Double.compare(hotnessSnapshot.get(o1), hotnessSnapshot.get(o2));
                if (comparison != 0)
                    return comparison;

                return 0;
            }
        });

        System.out.println(sstables);
        
        System.out.println(sstables.subList(1, sstables.size()));
    }

}
