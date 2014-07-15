package de.huberlin.wbi.dcs.workflow.scheduler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.SimEvent;

import de.huberlin.wbi.dcs.workflow.DataDependency;
import de.huberlin.wbi.dcs.workflow.Task;
import de.huberlin.wbi.dcs.workflow.Workflow;

/*
 * The StaticRoundRobinScheduler distributes tasks evenly among Vms prior to execution.
 */
public class StaticRoundRobinScheduler extends WorkflowScheduler {

	// stores, which vm id is assigned which task ids
	protected Map<Integer, ArrayList<Task>> assignedTasks;

	public StaticRoundRobinScheduler(String name, int taskSlotsPerVm) throws Exception {
		super(name, taskSlotsPerVm);
		assignedTasks = new HashMap<Integer, ArrayList<Task>>();
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
		for (int vmId : vms.keySet()) {
			assignedTasks.put(vmId, new ArrayList<Task>());
		}
		Iterator<Vm> it = vms.values().iterator();
		for (Workflow workflow : workflows) {
			for (Task task : workflow.getSortedTasks()) {
				if (!it.hasNext()) {
					it = vms.values().iterator();
				}
				Vm vm = it.next();
				assignedTasks.get(vm.getId()).add(task);
				Log.printLine(CloudSim.clock() + ": " + getName() + ": Assigning Task # " + task.getCloudletId() + " \"" + task.getName() + " " + task.getParams() + " \""  + " to VM # " + vm.getId());
			}
		}
		submitTasks();
	}
	
	protected void submitTasks() {
		int nIdleTaskSlots = idleTaskSlots.size();
		for (int i = 0; i < nIdleTaskSlots; i++) {
			Vm vm = idleTaskSlots.remove();
			ArrayList<Task> tasks = assignedTasks.get(vm.getId());
			boolean taskSubmitted = false;
			if (tasks != null) {
				for (int j = 0; j < tasks.size(); j++) {
					Task task = tasks.get(j);
					if (task.readyToExecute()) {
						tasks.remove(j);
						if (tasks.size() == 0) {
							assignedTasks.remove(vm.getId());
						}
						submitTask(task, vm);
						taskSubmitted = true;
						break;
					}
				}
			}
			if (!taskSubmitted) {
				idleTaskSlots.add(vm);
			}
		}
	}
	
	@Override
	protected void processCloudletReturn(SimEvent ev) {
		Task task = (Task) ev.getData();
		Vm vm = vms.get(task.getVmId());
		if (task.getCloudletStatus() == Cloudlet.SUCCESS) {
			idleTaskSlots.add(vm);
			Log.printLine(CloudSim.clock() + ": " + getName() + ": VM # " + vm.getId() + " completed Task # " + task.getCloudletId() + " \"" + task.getName() + " " + task.getParams());
			for (DataDependency outgoingEdge : task.getWorkflow().getGraph().getOutEdges(task)) {
				Task child = task.getWorkflow().getGraph().getDest(outgoingEdge);
				child.decNDataDependencies();
			}
		} else {
			Log.printLine(CloudSim.clock() + ": " + getName() + ": VM # " + vm.getId() + " encountered an error with Task # " + task.getCloudletId() + " \"" + task.getName() + " " + task.getParams() + " \"");
			resetTask(task);
			submitTask(task, vm);
		}

		if (assignedTasks.size() == 0) {
			Log.printLine(CloudSim.clock() + ": " + getName()
					+ ": All Tasks executed. Finishing...");
			clearDatacenters();
			finishExecution();
		} else {
			submitTasks();
		}
	}

}
