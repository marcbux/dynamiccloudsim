package de.huberlin.wbi.dcs.workflow.scheduler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;

import de.huberlin.wbi.dcs.workflow.Task;

/*
 * The StaticRoundRobinScheduler distributes tasks evenly among Vms prior to execution.
 */
public class StaticRoundRobinScheduler extends AbstractWorkflowScheduler {

	// stores, which vm id is assigned which task ids
	protected Map<Task, Vm> schedule;
	protected Map<Vm, Queue<Task>> readyTasks;
	private Iterator<Vm> vmIt;

	public StaticRoundRobinScheduler(String name, int taskSlotsPerVm)
			throws Exception {
		super(name, taskSlotsPerVm);
		schedule = new HashMap<>();
		readyTasks = new HashMap<>();
	}

	@Override
	public Task getNextTask(Vm vm) {
		Queue<Task> tasks = readyTasks.get(vm);
		if (tasks.size() > 0) {
			return tasks.remove();
		}
		return null;
	}

	@Override
	public void reschedule(Collection<Task> tasks, Collection<Vm> vms) {
		for (Vm vm : vms) {
			if (!readyTasks.containsKey(vm)) {
				Queue<Task> q = new LinkedList<>();
				readyTasks.put(vm, q);
				vmIt = vms.iterator();
			}
		}

		List<Task> sortedTasks = new ArrayList<>(tasks);
		Collections.sort(sortedTasks);

		for (Task task : sortedTasks) {
			if (!vmIt.hasNext()) {
				vmIt = vms.iterator();
			}
			Vm vm = vmIt.next();
			schedule.put(task, vm);
			Log.printLine(CloudSim.clock() + ": " + getName()
					+ ": Assigning Task # " + task.getCloudletId() + " \""
					+ task.getName() + " " + task.getParams() + " \""
					+ " to VM # " + vm.getId());
		}
	}

	@Override
	public void taskFailed(Task task, Vm vm) {
	}

	@Override
	public void taskReady(Task task) {
		readyTasks.get(schedule.get(task)).add(task);
	}

	@Override
	public boolean tasksRemaining() {
		for (Queue<Task> queue : readyTasks.values()) {
			if (!queue.isEmpty()) {
				return true;
			}
		}
		return false;
	}

	@Override
	public void taskSucceeded(Task task, Vm vm) {
	}

	@Override
	public void terminate() {
	}

}
