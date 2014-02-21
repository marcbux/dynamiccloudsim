package de.huberlin.dcs.workflow;

import java.util.Comparator;

public class TaskUpwardRankComparator implements Comparator<Task> {
	
	@Override
	public int compare(Task task1, Task task2) {
		return Double.compare(task2.getUpwardRank(), task1.getUpwardRank());
	}

}