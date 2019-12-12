package com.uob.edag.mybatis;

public class CountAndSumResult {
    private Integer count;
    private String sum;

    public Integer getCount() {
        return count;
    }

    public String getSum() {
        return sum;
    }

    @Override
    public String toString() {
        return "{count: " + getCount() + ", sum: " + getSum() + "}";
    }
}
