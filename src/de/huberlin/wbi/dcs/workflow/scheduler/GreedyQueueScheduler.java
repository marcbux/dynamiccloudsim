package de.huberlin.wbi.dcs.workflow.scheduler;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Queue;

import org.cloudbus.cloudsim.Vm;

import de.huberlin.wbi.dcs.workflow.Task;

/*
 * The JobQueueScheduler stores tasks, which are ready to execute, in a global queue.
 * Whenever a VM finishes execution of a task, it gets assigned a new task from the
 * head of the queue. Hence, no Vm processes more than one task at the same time.
 */
public class GreedyQueueScheduler extends AbstractWorkflowScheduler {

	protected Queue<Task> taskQueue;

	public GreedyQueueScheduler(String name, int taskSlotsPerVm)
			throws Exception {
		super(name, taskSlotsPerVm);
		taskQueue = new LinkedList<>();
	}

	@Override
	public void reschedule(Collection<Task> tasks, Collection<Vm> vms) {
	}

	@Override
	public void taskReady(Task task) {
		taskQueue.add(task);
	}

	@Override
	public void taskSucceeded(Task task, Vm vm) {
	}

	@Override
	public void taskFailed(Task task, Vm vm) {
	}

	@Override
	public boolean tasksRemaining() {
		return !taskQueue.isEmpty();
	}

	@Override
	public Task getNextTask(Vm vm) {
		return taskQueue.remove();
	}

	@Override
	public void terminate() {
	}

}
