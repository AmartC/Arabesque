package io.arabesque.utils;

import java.util.Comparator;

public class IntIntPairComparator implements Comparator<IntIntPair> {

    @Override
    public int compare(IntIntPair p1, IntIntPair p2) {
       if (p1.getFirst() < p2.getFirst())
          return -1;
       else if (p1.getFirst() > p2.getFirst())
          return 1;
       else
          return 0;
    }
}
