package de.huberlin.wbi.dcs.workflow.scheduler;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;

import de.huberlin.wbi.dcs.DynamicHost;
import de.huberlin.wbi.dcs.examples.Parameters;
import de.huberlin.wbi.dcs.workflow.DataDependency;
import de.huberlin.wbi.dcs.workflow.Task;

public abstract class AbstractReplicationScheduler extends AbstractWorkflowScheduler {

	// two collections of tasks, which are currently running;
	// note that the second collections is a subset of the first collection
	private Map<Integer, Task> tasks;
	private Map<Integer, Task> speculativeTasks;

	public AbstractReplicationScheduler(String name, int taskSlotsPerVm) throws Exception {
		super(name, taskSlotsPerVm);
		tasks = new HashMap<>();
		speculativeTasks = new HashMap<>();
	}

	private void submitSpeculativeTask(Task task, Vm vm) {
		Log.printLine(CloudSim.clock() + ": " + getName() + ": VM # " + vm.getId() + " starts executing speculative copy of Task # " + task.getCloudletId() + " \""
		    + task.getName() + " " + task.getParams() + " \"");
		task.setVmId(vm.getId());
		if (numGen.nextDouble() < Parameters.likelihoodOfFailure) {
			task.setScheduledToFail(true);
			task.setCloudletLength((long) (task.getCloudletLength() * Parameters.runtimeFactorInCaseOfFailure));
		} else {
			task.setScheduledToFail(false);
		}
		sendNow(getVmsToDatacentersMap().get(vm.getId()), CloudSimTags.CLOUDLET_SUBMIT, task);
	}

	@Override
	protected void submitTasks() {
		Queue<Vm> taskSlotsKeptIdle = new LinkedList<>();
		while (tasksRemaining() && !idleTaskSlots.isEmpty()) {
			Vm vm = idleTaskSlots.remove();
			Task task = getNextTask(vm);
			// task will be null if scheduler has tasks to be executed, but not
			// for this VM (e.g., if it abides to a static schedule or this VM
			// is a straggler, which the scheduler does not want to assign tasks
			// to)
			if (task == null) {
				taskSlotsKeptIdle.add(vm);
			} else if (tasks.containsKey(task.getCloudletId())) {
				speculativeTasks.put(task.getCloudletId(), task);
				submitSpeculativeTask(task, vm);
			} else {
				tasks.put(task.getCloudletId(), task);
				submitTask(task, vm);
			}
		}
		idleTaskSlots.addAll(taskSlotsKeptIdle);
	}

	@Override
	protected void processCloudletReturn(SimEvent ev) {
		// determine what kind of task was finished,
		Task task = (Task) ev.getData();
		Vm vm = availableVms.get(task.getVmId());
		Host host = vm.getHost();

		// if the task finished successfully, cancel its copy and remove them
		// both from internal data structures
		if (task.getCloudletStatus() == Cloudlet.SUCCESS) {
			Task originalTask = tasks.remove(task.getCloudletId());
			Task speculativeTask = speculativeTasks.remove(task.getCloudletId());

			if ((task.isSpeculativeCopy() && speculativeTask == null) || originalTask == null) {
				return;
			}

			if (task.isSpeculativeCopy()) {
				Log.printLine(CloudSim.clock() + ": " + getName() + ": VM # " + speculativeTask.getVmId() + " completed speculative copy of Task # "
				    + speculativeTask.getCloudletId() + " \"" + speculativeTask.getName() + " " + speculativeTask.getParams() + " \"");
				Log.printLine(CloudSim.clock() + ": " + getName() + ": VM # " + originalTask.getVmId() + " cancelled Task # " + originalTask.getCloudletId() + " \""
				    + originalTask.getName() + " " + originalTask.getParams() + " \"");
				availableVms.get(originalTask.getVmId()).getCloudletScheduler().cloudletCancel(originalTask.getCloudletId());
			} else {
				Log.printLine(CloudSim.clock() + ": " + getName() + ": VM # " + originalTask.getVmId() + " completed Task # " + originalTask.getCloudletId() + " \""
				    + originalTask.getName() + " " + originalTask.getParams() + " \"");
				if (speculativeTask != null) {
					Log.printLine(CloudSim.clock() + ": " + getName() + ": VM # " + speculativeTask.getVmId() + " cancelled speculative copy of Task # "
					    + speculativeTask.getCloudletId() + " \"" + speculativeTask.getName() + " " + speculativeTask.getParams() + " \"");
					availableVms.get(speculativeTask.getVmId()).getCloudletScheduler().cloudletCancel(speculativeTask.getCloudletId());
				}
			}

			// free task slots occupied by finished / cancelled tasks
			Vm originalVm = availableVms.get(originalTask.getVmId());
			idleTaskSlots.add(originalVm);
			taskSucceeded(originalTask, originalVm);
			if (speculativeTask != null) {
				Vm speculativeVm = availableVms.get(speculativeTask.getVmId());
				idleTaskSlots.add(speculativeVm);
				taskFailed(speculativeTask, speculativeVm);
			}

			// update the task queue by traversing the successor nodes in the
			// workflow
			for (DataDependency outgoingEdge : originalTask.getWorkflow().getGraph().getOutEdges(originalTask)) {
				if (host instanceof DynamicHost) {
					DynamicHost dHost = (DynamicHost) host;
					dHost.addFile(outgoingEdge.getFile());
				}
				Task child = originalTask.getWorkflow().getGraph().getDest(outgoingEdge);
				child.decNDataDependencies();
				if (child.readyToExecute()) {
					taskReady(child);
				}
			}

			// if the task finished unsuccessfully,
			// if it was a speculative task, just leave it be
			// otherwise, -- if it exists -- the speculative task becomes the
			// new original
		} else {
			Task speculativeTask = speculativeTasks.remove(task.getCloudletId());
			if (task.isSpeculativeCopy()) {
				Log.printLine(CloudSim.clock() + ": " + getName() + ": VM # " + task.getVmId() + " encountered an error with speculative copy of Task # "
				    + task.getCloudletId() + " \"" + task.getName() + " " + task.getParams() + " \"");
			} else {
				Log.printLine(CloudSim.clock() + ": " + getName() + ": VM # " + task.getVmId() + " encountered an error with Task # " + task.getCloudletId() + " \""
				    + task.getName() + " " + task.getParams() + " \"");
				tasks.remove(task.getCloudletId());
				if (speculativeTask != null) {
					speculativeTask.setSpeculativeCopy(false);
					tasks.put(speculativeTask.getCloudletId(), speculativeTask);
				} else {
					resetTask(task);
					taskReady(task);
				}
			}
			idleTaskSlots.add(vm);
			taskFailed(task, vm);
		}

		if (tasksRemaining()) {
			submitTasks();
		} else if (idleTaskSlots.size() == getVmsCreatedList().size() * getTaskSlotsPerVm()) {
			Log.printLine(CloudSim.clock() + ": " + getName() + ": All Tasks executed. Finishing...");
			terminate();
			clearDatacenters();
			finishExecution();
		}
	}

}