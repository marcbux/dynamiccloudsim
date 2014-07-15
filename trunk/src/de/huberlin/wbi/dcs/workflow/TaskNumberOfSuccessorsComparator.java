package de.huberlin.wbi.dcs.workflow;

import java.util.Comparator;

public class TaskNumberOfSuccessorsComparator implements Comparator<Task> {
	
	@Override
	public int compare(Task task1, Task task2) {
		return Double.compare(task2.getWorkflow().getGraph().getOutEdges(task2).size(), task1.getWorkflow().getGraph().getOutEdges(task1).size());
	}

}
