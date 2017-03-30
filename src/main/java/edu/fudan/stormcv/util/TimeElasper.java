package edu.fudan.stormcv.util;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


public class TimeElasper implements Serializable {
    /**
	 * 
	 */
	private static final long serialVersionUID = 6149534041006930094L;
	List<Integer> times;

    public TimeElasper() {
        times = new ArrayList<Integer>();
    }

    public void push(int time) {
        times.add(time);
    }

    public int getKSum(int k) {
        int sum = 0;
        for (int i = 0; i < k; i++) {
            sum += times.get(i);
        }
        return sum;
    }

    public double getKAve(int k) {
        int sum = getKSum(k);
        return (double) sum / (double) k;
    }
}
