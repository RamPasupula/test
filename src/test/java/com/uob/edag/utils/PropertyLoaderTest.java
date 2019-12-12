package com.uob.edag.utils;

public class PropertyLoaderTest {

	public static void main(String[] args) {
		System.out.println(PropertyLoader.getProperty("HADOOP_CLASSPATH"));
		System.out.println(PropertyLoader.getProperty("HADOOP_CLASSPATH", true));
	}
}
