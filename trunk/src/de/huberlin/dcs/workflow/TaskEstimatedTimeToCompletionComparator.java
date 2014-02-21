package de.huberlin.dcs.workflow;

import java.util.Comparator;

public class TaskEstimatedTimeToCompletionComparator implements Comparator<Task> {
	
	@Override
	public int compare(Task task1, Task task2) {
		return Double.compare(task1.getEstimatedTimeToCompletion(), task2.getEstimatedTimeToCompletion());
	}

}
