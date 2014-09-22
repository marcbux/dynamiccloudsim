package de.huberlin.wbi.dcs.workflow.scheduler;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Locale;
import java.util.Map;
import java.util.Queue;

import org.apache.commons.math3.distribution.NormalDistribution;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;

import de.huberlin.wbi.dcs.workflow.Task;

public class C2O extends AbstractWorkflowScheduler {

	public static boolean printEstimatesVsRuntimes = false;

	protected DecimalFormat df;

	protected Map<String, Queue<Task>> queuePerTask;

	protected Map<Vm, Map<String, WienerProcessModel>> runtimePerTaskPerVm;

	protected Map<Vm, WienerProcessModel> stageintimePerMBPerVm;

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

		protected final double percentile = 0.01;

		protected String taskName;
		protected int vmId;

		protected Deque<Runtime> measurements;
		protected Queue<Double> differences;
		protected double sumOfDifferences;
		protected Deque<Runtime> estimates;

		public WienerProcessModel(int vmId) {
			this("", vmId);
		}

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
		stageintimePerMBPerVm = new HashMap<>();
		this.runId = runId;
		Locale loc = new Locale("en");
		df = (DecimalFormat) NumberFormat.getNumberInstance(loc);
		df.applyPattern("###.####");
		df.setMaximumIntegerDigits(7);
	}

	@Override
	public void reschedule(Collection<Task> tasks, Collection<Vm> vms) {
		for (Vm vm : vms) {
			if (!runtimePerTaskPerVm.containsKey(vm)) {
				Map<String, WienerProcessModel> runtimePerTask = new HashMap<>();
				runtimePerTaskPerVm.put(vm, runtimePerTask);
				stageintimePerMBPerVm.put(vm,
						new WienerProcessModel(vm.getId()));
			}
		}
		for (Task task : tasks) {
			if (!queuePerTask.containsKey(task.getName())) {
				Queue<Task> q = new LinkedList<>();
				queuePerTask.put(task.getName(), q);
				for (Vm vm : vms) {
					runtimePerTaskPerVm.get(vm).put(task.getName(),
							new WienerProcessModel(task.getName(), vm.getId()));
				}
			}
		}
	}

	@Override
	public Task getNextTask(Vm vm) {
		// for this VM, compute the runtime of each task, relative to the
		// other VMs; then, select the task with the lowest (relative)
		// runtime
		double minRelativeEstimate = Double.MAX_VALUE;
		String selectedTask = "";
		for (String task : queuePerTask.keySet()) {
			if (!queuePerTask.get(task).isEmpty()) {
				double estimate = runtimePerTaskPerVm.get(vm).get(task)
						.getEstimate(CloudSim.clock());
				double relativeEstimate = 0d;

				if (estimate > 0) {
					double sumOfEstimates = 0d;
					int numEstimates = 0;
					for (Vm runningVm : runtimePerTaskPerVm.keySet()) {
						double runningEstimate = (runningVm.getId() == vm
								.getId()) ? estimate : runtimePerTaskPerVm
								.get(runningVm).get(task)
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

		return (queuePerTask.get(selectedTask).remove());
	}

	@Override
	public void taskReady(Task task) {
		queuePerTask.get(task.getName()).add(task);
	}

	@Override
	public boolean tasksRemaining() {
		for (Queue<Task> queue : queuePerTask.values()) {
			if (!queue.isEmpty()) {
				return true;
			}
		}
		return false;
	}

	@Override
	public void taskSucceeded(Task task, Vm vm) {
		double runtime = task.getFinishTime() - task.getExecStartTime();
		runtimePerTaskPerVm.get(vm).get(task.getName())
				.addRuntime(task.getFinishTime(), runtime);
		// double stageintimePerMB = 0d;
		// vm.getCloudletScheduler();
	}

	@Override
	public void taskFailed(Task task, Vm vm) {
	}

	@Override
	public void terminate() {
		if (printEstimatesVsRuntimes) {
			for (Map<String, WienerProcessModel> m : runtimePerTaskPerVm
					.values()) {
				for (WienerProcessModel w : m.values()) {
					w.printEstimatesVsRuntimes();
				}
			}
			for (WienerProcessModel w : stageintimePerMBPerVm.values()) {
				w.printEstimatesVsRuntimes();
			}
		}
	}

}
