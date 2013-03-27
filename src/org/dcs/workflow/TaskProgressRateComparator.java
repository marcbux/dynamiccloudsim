package org.dcs.workflow;

import java.util.Comparator;

public class TaskProgressRateComparator implements Comparator<Task> {
	
	@Override
	public int compare(Task task1, Task task2) {
		return Double.compare(task1.getProgressRate(), task2.getProgressRate());
	}

}
