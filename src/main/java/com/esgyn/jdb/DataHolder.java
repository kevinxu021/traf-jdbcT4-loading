package com.esgyn.jdb;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CountDownLatch;

public class DataHolder {
	private static final ConcurrentLinkedDeque<List> data = new ConcurrentLinkedDeque<List>();
	private static CountDownLatch cdl = new CountDownLatch(1);
	private static boolean done = false;

	public static void main(String[] args) {

	}

	static void push(List e) {
		data.push(e);
	}

	static List pop() {
		return data.pollFirst();
	}
	
	static int size(){
		return data.size();
	}

	static CountDownLatch getCountDownLatch() {
		return cdl;
	}

	public static boolean isDone() {
		return done;
	}

	public static void setDone(boolean done) {
		DataHolder.done = done;
	}

	public static void resetCountDownLatch() {
		cdl = new CountDownLatch(1);
	}

}
