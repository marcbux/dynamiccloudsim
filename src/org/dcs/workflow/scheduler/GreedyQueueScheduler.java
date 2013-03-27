package org.dcs.workflow.scheduler;

import java.util.LinkedList;
import java.util.Queue;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.SimEvent;
import org.dcs.workflow.DataDependency;
import org.dcs.workflow.Task;
import org.dcs.workflow.Workflow;

/*
 * The JobQueueScheduler stores tasks, which are ready to execute, in a global queue.
 * Whenever a VM finishes execution of a task, it gets assigned a new task from the
 * head of the queue. Hence, no Vm processes more than one task at the same time.
 */
public class GreedyQueueScheduler extends WorkflowScheduler {

	protected Queue<Task> taskQueue;
	
	public GreedyQueueScheduler(String name, int taskSlotsPerVm) throws Exception {
		super(name, taskSlotsPerVm);
		taskQueue = new LinkedList<Task>();
	}
	
	protected void registerTasks() {
		for (Workflow workflow : workflows) {
			for (Task task : workflow.getSortedTasks()) {
				if (task.readyToExecute()) {
					taskQueue.add(task);
				}
			}
		}
	}
	
	/**
	 * Submit cloudlets to the created VMs. This function is called after Vms
	 * have been created.
	 * 
	 * @pre $none
	 * @post $none
	 */
	@Override
	protected void submitCloudlets() {
		super.registerVms();
		registerTasks();
		submitTasks();
	}
	
	protected void submitTasks() {
		while (!taskQueue.isEmpty() && !idleTaskSlots.isEmpty()) {
			submitTask(taskQueue.remove(), idleTaskSlots.remove());
		}
	}

	@Override
	protected void processCloudletReturn(SimEvent ev) {
		Task task = (Task) ev.getData();
		Vm vm = vms.get(task.getVmId());
		idleTaskSlots.add(vm);
		if (task.getCloudletStatus() == Cloudlet.SUCCESS) {
			Log.printLine(CloudSim.clock() + ": " + getName() + ": VM # " + vm.getId() + " completed Task # " + task.getCloudletId() + " \"" + task.getName() + " " + task.getParams() + " \"");
			for (DataDependency outgoingEdge : task.getWorkflow().getGraph().getOutEdges(task)) {
				Task child = task.getWorkflow().getGraph().getDest(outgoingEdge);
				child.decNDataDependencies();
				if (child.readyToExecute()) {
					taskQueue.add(child);
				}
			}
		} else {
			Log.printLine(CloudSim.clock() + ": " + getName() + ": VM # " + vm.getId() + " encountered an error with Task # " + task.getCloudletId() + " \"" + task.getName() + " " + task.getParams() + " \"");
			resetTask(task);
			taskQueue.add(task);
		}

		if (taskQueue.size() > 0) {
			submitTasks();
		} else if (idleTaskSlots.size() == getVmsCreatedList().size() * getTaskSlotsPerVm()) {
			Log.printLine(CloudSim.clock() + ": " + getName()
					+ ": All Tasks executed. Finishing...");
			clearDatacenters();
			finishExecution();
		}
	}

}
