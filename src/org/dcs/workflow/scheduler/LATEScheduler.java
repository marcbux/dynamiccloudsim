package org.dcs.workflow.scheduler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;
import org.dcs.examples.Parameters;
import org.dcs.workflow.DataDependency;
import org.dcs.workflow.Task;
import org.dcs.workflow.TaskEstimatedTimeToCompletionComparator;
import org.dcs.workflow.TaskProgressRateComparator;

public class LATEScheduler extends GreedyQueueScheduler {
	
	// a node is slow if the sum of progress scores for all succeeded and in-progress tasks on the node is below this threshold
	// speculative tasks are not executed on slow nodes
	// default: 25th percentile of node progress rates
	protected final double slowNodeThreshold = 0.25;
	protected double currentSlowNodeThreshold;
	
	// A threshold that a task's progress rate is compared with to determine whether it is slow enough to be speculated upon
	// default: 25th percentile of task progress rates
	protected final double slowTaskThreshold = 0.25;
	protected double currentSlowTaskThreshold;
	
	// a cap on the number of speculative tasks that can be running at once (given as a percentage of task slots)
	// default: 10% of available task slots
	protected final double speculativeCap = 0.1;
	protected int speculativeCapAbs;
	
	// two collections of tasks, which are currently running;
	// note that the second collections is a subset of the first collection
	protected Map<Integer, Task> tasks;
	protected Map<Integer, Task> speculativeTasks;
	
	protected Map<Integer, Double> nSucceededTasksPerVm;
	
	public LATEScheduler(String name, int taskSlotsPerVm) throws Exception {
		super(name, taskSlotsPerVm);
		tasks = new HashMap<Integer, Task>();
		speculativeTasks = new HashMap<Integer, Task>();
		nSucceededTasksPerVm = new HashMap<Integer, Double>();
	}
	
	@Override
	protected void submitCloudlets() {
		super.registerVms();
		speculativeCapAbs = (int)(vms.size() * taskSlotsPerVm * speculativeCap + 0.5);
		for (int id : vms.keySet()) {
			nSucceededTasksPerVm.put(id, 0d);
		}
		super.registerTasks();
		submitTasks();
	}
	
	public class VmSumOfProgressScoresComparator implements Comparator<Vm> {
		
		Map<Integer, Double> vmIdToSumOfProgressScores;
		
		public VmSumOfProgressScoresComparator(Map<Integer, Double> vmIdToSumOfProgressScores) {
			this.vmIdToSumOfProgressScores = vmIdToSumOfProgressScores;
		}
		
		@Override
		public int compare(Vm vm1, Vm vm2) {			
			return Double.compare(vmIdToSumOfProgressScores.get(vm1.getId()), vmIdToSumOfProgressScores.get(vm2.getId()));
		}

	}
	
	@Override
	protected void submitTasks() {
		// compute the sum of progress scores for all vms
		Map<Integer, Double> vmIdToSumOfProgressScores = new HashMap<Integer, Double>();
		for (Vm vm : vms.values()) {
			vmIdToSumOfProgressScores.put(vm.getId(), (double)nSucceededTasksPerVm.get(vm.getId()));
		}
		for (Task t : tasks.values()) {
			t.computeEstimatedTimeToCompletion();
			vmIdToSumOfProgressScores.put(t.getVmId(), vmIdToSumOfProgressScores.get(t.getVmId()) + t.getProgressScore());
		}
		
		// compute the quantiles of task and node slowness
		List<Vm> runningVms = new ArrayList<Vm>(vms.values());
		Collections.sort(runningVms, new VmSumOfProgressScoresComparator(vmIdToSumOfProgressScores));
		int quantileIndex = (int)(runningVms.size() * slowNodeThreshold - 0.5);
		currentSlowNodeThreshold = vmIdToSumOfProgressScores.get(runningVms.get(quantileIndex).getId());
		
		List<Task> runningTasks = new ArrayList<Task>(tasks.values());
		Collections.sort(runningTasks, new TaskProgressRateComparator());
		quantileIndex = (int)(runningTasks.size() * slowTaskThreshold - 0.5);
		currentSlowTaskThreshold = (runningTasks.size() > 0) ? runningTasks.get(quantileIndex).getProgressRate() : -1;
		
		int nIdleTaskSlots = idleTaskSlots.size();
		for (int i = 0; i < nIdleTaskSlots; i++) {
			Vm vm = idleTaskSlots.remove();
			
			// check whether a task is to be executed speculatively or regularly
			if (tasks.size() > speculativeTasks.size() && speculativeTasks.size() < speculativeCapAbs && vmIdToSumOfProgressScores.get(vm.getId()) > currentSlowNodeThreshold) {
				List<Task> nonSpeculativeTasks = new ArrayList<Task>(tasks.values());
				nonSpeculativeTasks.removeAll(speculativeTasks.values());
				Collections.sort(nonSpeculativeTasks, new TaskEstimatedTimeToCompletionComparator());
				
				Task task = nonSpeculativeTasks.get(nonSpeculativeTasks.size() - 1);
				Task speculativeTask = new Task(task);
				speculativeTask.setSpeculativeCopy(true);
				speculativeTasks.put(speculativeTask.getCloudletId(), speculativeTask);
				submitSpeculativeTask(speculativeTask, vm);
			} else if (!taskQueue.isEmpty()) {
				Task task = taskQueue.remove();
				tasks.put(task.getCloudletId(), task);
				submitTask(task, vm);
			} else {
				idleTaskSlots.add(vm);
			}
		}
	}
	
	protected void submitSpeculativeTask(Task task, Vm vm) {
		Log.printLine(CloudSim.clock() + ": " + getName() + ": VM # "
				+ vm.getId() + " starts executing speculative copy of Task # " + task.getCloudletId() + " \"" + task.getName() + " " + task.getParams() + " \"");
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
	
	@Override
	protected void processCloudletReturn(SimEvent ev) {
		// determine what kind of task was finished, 
		Task task = (Task) ev.getData();
		
		// if the task finished successfully, cancel its copy and remove them both from internal data structures
		if (task.getCloudletStatus() == Cloudlet.SUCCESS) {
			Task originalTask = tasks.remove(task.getCloudletId());
			Task speculativeTask = speculativeTasks.remove(task.getCloudletId());
			
			if ((task.isSpeculativeCopy() && speculativeTask == null) || originalTask == null) {
				return;
			}
			
			if (task.isSpeculativeCopy()) {
				Log.printLine(CloudSim.clock() + ": " + getName() + ": VM # " + speculativeTask.getVmId() + " completed speculative copy of Task # " + speculativeTask.getCloudletId() + " \"" + speculativeTask.getName() + " " + speculativeTask.getParams() + " \"");
				Log.printLine(CloudSim.clock() + ": " + getName() + ": VM # " + originalTask.getVmId() + " cancelled Task # " + originalTask.getCloudletId() + " \"" + originalTask.getName() + " " + originalTask.getParams() + " \"");
				vms.get(originalTask.getVmId()).getCloudletScheduler().cloudletCancel(originalTask.getCloudletId());
			} else {
				Log.printLine(CloudSim.clock() + ": " + getName() + ": VM # " + originalTask.getVmId() + " completed Task # " + originalTask.getCloudletId() + " \"" + originalTask.getName() + " " + originalTask.getParams() + " \"");
				if (speculativeTask != null) {
					Log.printLine(CloudSim.clock() + ": " + getName() + ": VM # " + speculativeTask.getVmId() + " cancelled speculative copy of Task # " + speculativeTask.getCloudletId() + " \"" + speculativeTask.getName() + " " + speculativeTask.getParams() + " \"");
					vms.get(speculativeTask.getVmId()).getCloudletScheduler().cloudletCancel(speculativeTask.getCloudletId());
				}
			}
			
			// free task slots occupied by finished / cancelled tasks
			idleTaskSlots.add(vms.get(originalTask.getVmId()));
			nSucceededTasksPerVm.put(originalTask.getVmId(), nSucceededTasksPerVm.get(originalTask.getVmId()) + originalTask.getProgressScore());
			if (speculativeTask != null) {
				idleTaskSlots.add(vms.get(speculativeTask.getVmId()));
				nSucceededTasksPerVm.put(speculativeTask.getVmId(), nSucceededTasksPerVm.get(speculativeTask.getVmId()) + speculativeTask.getProgressScore());
			}
			
			// update the task queue by traversing the successor nodes in the workflow
			for (DataDependency outgoingEdge : originalTask.getWorkflow().getGraph().getOutEdges(originalTask)) {
				Task child = originalTask.getWorkflow().getGraph().getDest(outgoingEdge);
				child.decNDataDependencies();
				if (child.readyToExecute()) {
					taskQueue.add(child);
				}
			}
			
		// if the task finished unsuccessfully,
		//   if it was a speculative task, just leave it be
		//   otherwise, -- if it exists -- the speculative task becomes the new original
		} else {
			
			Task speculativeTask = speculativeTasks.remove(task.getCloudletId());
			if (task.isSpeculativeCopy()) {
				Log.printLine(CloudSim.clock() + ": " + getName() + ": VM # " + task.getVmId() + " encountered an error with speculative copy of Task # " + task.getCloudletId() + " \"" + task.getName() + " " + task.getParams() + " \"");
			} else {
				Log.printLine(CloudSim.clock() + ": " + getName() + ": VM # " + task.getVmId() + " encountered an error with Task # " + task.getCloudletId() + " \"" + task.getName() + " " + task.getParams() + " \"");
				tasks.remove(task.getCloudletId());
				if (speculativeTask != null) {
					speculativeTask.setSpeculativeCopy(false);
					tasks.put(speculativeTask.getCloudletId(), speculativeTask);
				} else {
					resetTask(task);
					taskQueue.add(task);
				}
			}
			idleTaskSlots.add(vms.get(task.getVmId()));
			nSucceededTasksPerVm.put(task.getVmId(), nSucceededTasksPerVm.get(task.getVmId()) + task.getProgressScore());
		}
		
		// finish if there are no more tasks to be executed
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
