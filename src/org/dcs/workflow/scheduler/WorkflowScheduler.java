package org.dcs.workflow.scheduler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.dcs.DynamicVm;
import org.dcs.examples.Parameters;
import org.dcs.workflow.Task;
import org.dcs.workflow.Workflow;

public abstract class WorkflowScheduler extends DatacenterBroker {
	
	protected List<Workflow> workflows;
	protected Map<Integer, Vm> vms;
	protected int taskSlotsPerVm;
	protected Queue<Vm> idleTaskSlots;
	private double runtime;
	
	public WorkflowScheduler(String name, int taskSlotsPerVm) throws Exception {
		super(name);
		workflows = new ArrayList<Workflow>();
		vms = new HashMap<Integer, Vm>();
		this.taskSlotsPerVm = taskSlotsPerVm;
		idleTaskSlots = new LinkedList<Vm>();
	}
	
	protected void registerVms() {
		for (Vm vm : getVmsCreatedList()) {
			vms.put(vm.getId(), vm);
			for (int i = 0; i < getTaskSlotsPerVm(); i++) {
				idleTaskSlots.add(vm);
			}
		}
	}
	
	public List<Workflow> getWorkflows() {
		return workflows;
	}
	
	public int getTaskSlotsPerVm() {
		return taskSlotsPerVm;
	}
	
	public void submitWorkflow(Workflow workflow) {
		workflows.add(workflow);
	}
	
	protected void submitTask(Task task, Vm vm) {
		Log.printLine(CloudSim.clock() + ": " + getName() + ": VM # "
				+ vm.getId() + " starts executing Task # " + task.getCloudletId() + " \"" + task.getName() + " " + task.getParams() + " \"");
		task.setVmId(vm.getId());
		if (Parameters.numGen.nextDouble() < Parameters.likelihoodOfFailure) {
			task.setScheduledToFail(true);
			task.setCloudletLength((long)(task.getCloudletLength() * Parameters.runtimeFactorInCaseOfFailure));
		} else {
			task.setScheduledToFail(false);
		}
		sendNow(getVmsToDatacentersMap().get(vm.getId()),
					CloudSimTags.CLOUDLET_SUBMIT, task);
	}
	
	protected void clearDatacenters() {
		for (Vm vm : getVmsCreatedList()) {
			if (vm instanceof DynamicVm) {
				DynamicVm dVm = (DynamicVm)vm;
				dVm.closePerformanceLog();
			}
		}
		super.clearDatacenters();
		runtime = CloudSim.clock();
	}
	
	public double getRuntime() {
		return runtime;
	}
	
	protected void resetTask(Task task) {
		task.setCloudletFinishedSoFar(0);
		try {
			task.setCloudletStatus(Cloudlet.CREATED);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
