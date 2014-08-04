package de.huberlin.wbi.dcs.workflow.scheduler;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import org.apache.commons.math3.distribution.NormalDistribution;
import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.SimEvent;

import de.huberlin.wbi.dcs.workflow.DataDependency;
import de.huberlin.wbi.dcs.workflow.Task;
import de.huberlin.wbi.dcs.workflow.Workflow;

public class C2O extends WorkflowScheduler {

	protected Map<String, Queue<Task>> queuePerTask;

	protected Map<Integer, Map<String, WienerProcessModel>> runtimePerTaskPerVm;

	public class WienerProcessModel {

		double lastFinish = 0d;
		double lastRuntime = 0d;
		int numFinished = 0;

		double sigma = 0d;

		public void addRuntime(double finish, double runtime) {
			runtime = Math.log(runtime);
			if (numFinished > 0) {
				sigma += Math.pow((runtime - lastRuntime), 2d)
						/ (finish - lastFinish);
			}
			lastFinish = finish;
			lastRuntime = runtime;
			numFinished++;
		}

		public double getRuntime(double time) {
			switch (numFinished) {
			case 0:
				return 0d;
			case 1:
				return Double.MIN_VALUE;
			default:
				double sd = (sigma / (numFinished - 1)) * (time - lastFinish);
				double estimatedRuntime = (sd == 0d) ? lastRuntime
						: new NormalDistribution(lastRuntime, sd)
								.inverseCumulativeProbability(0.25);
				return Math.pow(Math.E, estimatedRuntime);
			}
		}
	}

	public C2O(String name, int taskSlotsPerVm) throws Exception {
		super(name, taskSlotsPerVm);
		queuePerTask = new HashMap<>();
		runtimePerTaskPerVm = new HashMap<>();
	}

	@Override
	protected void submitCloudlets() {
		registerVms();
		registerTasks();
		submitTasks();
	}

	@Override
	protected void registerVms() {
		super.registerVms();
		for (int id : vms.keySet()) {
			Map<String, WienerProcessModel> runtimePerTask = new HashMap<>();
			runtimePerTaskPerVm.put(id, runtimePerTask);
		}
	}

	protected void registerTasks() {
		for (Workflow workflow : workflows) {
			for (Task task : workflow.getSortedTasks()) {
				if (!queuePerTask.containsKey(task.getName())) {
					Queue<Task> q = new LinkedList<>();
					queuePerTask.put(task.getName(), q);
					for (int id : vms.keySet()) {
						runtimePerTaskPerVm.get(id).put(task.getName(),
								new WienerProcessModel());
					}
				}

				if (task.readyToExecute()) {
					queuePerTask.get(task.getName()).add(task);
				}
			}
		}
	}

	protected void submitTasks() {
		while (!idleTaskSlots.isEmpty()) {
			if (!tasksInQueue()) {
				break;
			}
			Vm vm = idleTaskSlots.remove();

			// for this VM, compute the runtime of each task, relative to the
			// other VMs; then, select the task with the lowest (relative)
			// runtime
			double minRelativeRuntime = Double.MAX_VALUE;
			String selectedTask = "";
			for (String task : queuePerTask.keySet()) {
				if (!queuePerTask.get(task).isEmpty()) {
					double sumOfRuntimes = 0d;
					for (Integer vmId : vms.keySet()) {
						sumOfRuntimes += runtimePerTaskPerVm.get(vmId)
								.get(task).getRuntime(CloudSim.clock());
					}
					double relativeRuntime = (sumOfRuntimes == 0d) ? 0d
							: runtimePerTaskPerVm.get(vm.getId()).get(task)
									.getRuntime(CloudSim.clock())
									/ sumOfRuntimes;
					if (relativeRuntime < minRelativeRuntime) {
						minRelativeRuntime = relativeRuntime;
						selectedTask = task;
					}
				}
			}

			submitTask(queuePerTask.get(selectedTask).remove(), vm);
		}
	}

	private boolean tasksInQueue() {
		for (Queue<Task> queue : queuePerTask.values()) {
			if (!queue.isEmpty()) {
				return true;
			}
		}
		return false;
	}

	@Override
	protected void processCloudletReturn(SimEvent ev) {
		Task task = (Task) ev.getData();
		Vm vm = vms.get(task.getVmId());
		idleTaskSlots.add(vm);
		if (task.getCloudletStatus() == Cloudlet.SUCCESS) {
			Log.printLine(CloudSim.clock() + ": " + getName() + ": VM # "
					+ vm.getId() + " completed Task # " + task.getCloudletId()
					+ " \"" + task.getName() + " " + task.getParams() + " \"");

			double runtime = task.getFinishTime() - task.getExecStartTime();
			runtimePerTaskPerVm.get(vm.getId()).get(task.getName())
					.addRuntime(task.getFinishTime(), runtime);

			for (DataDependency outgoingEdge : task.getWorkflow().getGraph()
					.getOutEdges(task)) {
				Task child = task.getWorkflow().getGraph()
						.getDest(outgoingEdge);
				child.decNDataDependencies();
				if (child.readyToExecute()) {
					queuePerTask.get(child.getName()).add(child);
				}
			}
		} else {
			Log.printLine(CloudSim.clock() + ": " + getName() + ": VM # "
					+ vm.getId() + " encountered an error with Task # "
					+ task.getCloudletId() + " \"" + task.getName() + " "
					+ task.getParams() + " \"");
			resetTask(task);
			queuePerTask.get(task.getName()).add(task);
		}

		if (tasksInQueue()) {
			submitTasks();
		} else if (idleTaskSlots.size() == getVmsCreatedList().size()
				* getTaskSlotsPerVm()) {
			Log.printLine(CloudSim.clock() + ": " + getName()
					+ ": All Tasks executed. Finishing...");
			clearDatacenters();
			finishExecution();
		}
	}

}
