package edu.bu.cs755;

import java.io.IOException;
import java.util.List;


public class Main {

	public static void main(String[] args) throws IOException {
		
		
		StreamInput readInput = new StreamInput();
		List<String> Top5000List = readInput.getTop5000List();
		
		Task1 task1 = new Task1();
		task1.frequencyPosition(Top5000List);
		
		Task2 task2 = new Task2(Top5000List);
		task2.top20ranklist();
		

	}
}
