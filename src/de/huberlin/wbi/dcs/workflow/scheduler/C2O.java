package de.huberlin.wbi.dcs.workflow.scheduler;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Locale;
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

	public static boolean printEstimatesVsRuntimes = true;

	protected DecimalFormat df;

	protected Map<String, Queue<Task>> queuePerTask;

	protected Map<Integer, Map<String, WienerProcessModel>> runtimePerTaskPerVm;

	protected int runId;

	public class Runtime {
		private final double timestamp;
		private final double runtime;

		public Runtime(double timestamp, double runtime) {
			this.timestamp = timestamp;
			this.runtime = runtime;
		}

		public double getRuntime() {
			return runtime;
		}

		public double getTimestamp() {
			return timestamp;
		}

		@Override
		public String toString() {
			return df.format(timestamp) + "," + df.format(runtime);
		}
	}

	public class WienerProcessModel {

		protected final double percentile = 0.25;

		protected String taskName;
		protected int vmId;

		protected Deque<Runtime> measurements;
		protected Queue<Double> differences;
		protected double sumOfDifferences;
		protected Deque<Runtime> estimates;

		public WienerProcessModel(String taskName, int vmId) {
			this.taskName = taskName;
			this.vmId = vmId;
			measurements = new LinkedList<>();
			differences = new LinkedList<>();
			estimates = new LinkedList<>();
		}

		public void printEstimatesVsRuntimes() {
			try (BufferedWriter writer = new BufferedWriter(new FileWriter(
					"run" + runId + "_vm" + vmId + "_" + taskName + ".csv"))) {
				writer.write("time;estimate;measurement\n");
				for (Runtime measurement : measurements) {
					writer.write(df.format(measurement.getTimestamp() / 60)
							+ ";;"
							+ df.format(Math.pow(Math.E,
									measurement.getRuntime()) / 60) + "\n");
				}
				for (Runtime estimate : estimates) {
					writer.write(df.format(estimate.getTimestamp() / 60)
							+ ";"
							+ df.format(Math.pow(Math.E, estimate.getRuntime()) / 60)
							+ ";\n");
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		public void addRuntime(double timestamp, double runtime) {
			Runtime measurement = new Runtime(timestamp, Math.log(runtime));
			if (!measurements.isEmpty()) {
				Runtime lastMeasurement = measurements.getLast();
				double difference = (measurement.getRuntime() - lastMeasurement
						.getRuntime())
						/ (measurement.getTimestamp() - lastMeasurement
								.getTimestamp());
				sumOfDifferences += difference;
				differences.add(difference);
			}
			measurements.add(measurement);
		}

		public double getEstimate(double timestamp) {
			if (differences.size() < 2) {
				return 0d;
			}

			Runtime lastMeasurement = measurements.getLast();
			double variance = 0d;
			double avgDifference = sumOfDifferences / differences.size();
			for (double difference : differences) {
				variance += Math.pow(difference - avgDifference, 2d);
			}
			variance /= differences.size() - 1;
			variance *= timestamp - lastMeasurement.getTimestamp();

			double estimate = lastMeasurement.getRuntime();

			if (variance > 0d) {
				NormalDistribution nd = new NormalDistribution(
						lastMeasurement.getRuntime(), Math.sqrt(variance));
				estimate = nd.inverseCumulativeProbability(percentile);

			}

			Runtime runtime = new Runtime(timestamp, estimate);

			estimates.add(runtime);

			return Math.pow(Math.E, estimate);
		}
	}

	public C2O(String name, int taskSlotsPerVm, int runId) throws Exception {
		super(name, taskSlotsPerVm);
		queuePerTask = new HashMap<>();
		runtimePerTaskPerVm = new HashMap<>();
		this.runId = runId;
		Locale loc = new Locale("en");
		df = (DecimalFormat) NumberFormat.getNumberInstance(loc);
		df.applyPattern("###.####");
		df.setMaximumIntegerDigits(7);
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
								new WienerProcessModel(task.getName(), id));
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
			double minRelativeEstimate = Double.MAX_VALUE;
			String selectedTask = "";
			for (String task : queuePerTask.keySet()) {
				if (!queuePerTask.get(task).isEmpty()) {
					double estimate = runtimePerTaskPerVm.get(vm.getId())
							.get(task).getEstimate(CloudSim.clock());
					double relativeEstimate = 0d;

					if (estimate > 0) {
						double sumOfEstimates = 0d;
						int numEstimates = 0;
						for (Integer vmId : vms.keySet()) {
							double runningEstimate = (vmId == vm.getId()) ? estimate
									: runtimePerTaskPerVm.get(vmId).get(task)
											.getEstimate(CloudSim.clock());
							if (runningEstimate > 0) {
								sumOfEstimates += runningEstimate;
								numEstimates++;
							}
						}
						double avgEstimate = sumOfEstimates / numEstimates;
						relativeEstimate = estimate / avgEstimate;
					}

					if (relativeEstimate < minRelativeEstimate) {
						minRelativeEstimate = relativeEstimate;
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
			for (Map<String, WienerProcessModel> m : runtimePerTaskPerVm
					.values()) {
				for (WienerProcessModel w : m.values()) {
					w.printEstimatesVsRuntimes();
				}
			}
			clearDatacenters();
			finishExecution();
		}
	}

}
