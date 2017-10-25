package de.huberlin.wbi.dcs.workflow.scheduler;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;

import org.apache.commons.math3.distribution.NormalDistribution;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;

import de.huberlin.wbi.dcs.workflow.Task;

public class ERA extends AbstractReplicationScheduler {

	public static double alpha = 0.2;
	public static int rho = 1;

	public static boolean printEstimatesVsRuntimes = false;
	public static boolean logarithmize = false;

	protected DecimalFormat df;
	protected int runId;

	protected Map<String, Queue<Task>> readyTasksPerBot;
	protected Map<String, Set<Task>> runningTasksPerBot;
	protected Map<Vm, Map<String, WienerProcessModel>> runtimePerBotPerVm;

	protected List<Task> replicas;
	protected Set<Task> todo;

	public class Runtime {
		public final double timestamp;
		public final double runtime;

		public Runtime(double timestamp, double runtime) {
			this.timestamp = timestamp;
			this.runtime = runtime;
		}

		@Override
		public String toString() {
			return df.format(timestamp) + "," + df.format(runtime);
		}
	}

	public class WienerProcessModel {
		protected String botName;
		protected int vmId;

		protected Deque<Runtime> measurements;
		protected Queue<Double> differences;
		protected double sumOfDifferences;
		protected Deque<Runtime> estimates;

		public WienerProcessModel(String botName, int vmId) {
			this.botName = botName;
			this.vmId = vmId;
			measurements = new LinkedList<>();
			differences = new LinkedList<>();
			estimates = new LinkedList<>();
		}

		public void printEstimatesVsRuntimes() {
			String filename = "run" + runId + "_vm" + vmId + "_" + botName + ".csv";
			try (BufferedWriter writer = new BufferedWriter(new FileWriter(filename))) {
				writer.write("time;estimate;measurement\n");
				for (Runtime measurement : measurements) {
					writer.write(df.format(measurement.timestamp / 60) + ";;" + df.format(measurement.runtime / 60) + "\n");
				}
				for (Runtime estimate : estimates) {
					writer.write(df.format(estimate.timestamp / 60) + ";" + df.format(estimate.runtime / 60) + ";\n");
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		public void addRuntime(double timestamp, double runtime) {
			Runtime measurement = new Runtime(timestamp, logarithmize ? Math.log(runtime) : runtime);
			if (!measurements.isEmpty()) {
				Runtime lastMeasurement = measurements.getLast();
				double difference = (measurement.runtime - lastMeasurement.runtime) / Math.sqrt(measurement.timestamp - lastMeasurement.timestamp);
				sumOfDifferences += difference;
				differences.add(difference);
			}
			measurements.add(measurement);
		}

		public double getEstimate(double timestamp, double quantile) {
			if (quantile == 0.5 && measurements.size() > 0) {
				return logarithmize ? Math.pow(Math.E, measurements.getLast().runtime) : Math.max(measurements.getLast().runtime, Double.MIN_NORMAL);
			}

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

			variance *= timestamp - lastMeasurement.timestamp;

			double estimate = lastMeasurement.runtime;
			if (variance > 0d) {
				NormalDistribution nd = new NormalDistribution(lastMeasurement.runtime, Math.sqrt(variance));
				estimate = nd.inverseCumulativeProbability(quantile);
			}

			estimate = logarithmize ? Math.pow(Math.E, estimate) : Math.max(estimate, Double.MIN_NORMAL);

			if (printEstimatesVsRuntimes) {
				Runtime runtime = new Runtime(timestamp, estimate);
				estimates.add(runtime);
			}
			return estimate;
		}
	}

	public ERA(String name, int taskSlotsPerVm, int runId) throws Exception {
		super(name, taskSlotsPerVm);
		readyTasksPerBot = new HashMap<>();
		runningTasksPerBot = new HashMap<>();
		runtimePerBotPerVm = new HashMap<>();
		replicas = new LinkedList<>();
		this.runId = runId;
		Locale loc = new Locale("en");
		df = (DecimalFormat) NumberFormat.getNumberInstance(loc);
		df.applyPattern("###.####");
		df.setMaximumIntegerDigits(7);
	}

	// this function is called once at the beginning of workflow execution
	@Override
	public void reschedule(Collection<Task> tasks, Collection<Vm> vms) {
		for (Vm vm : vms) {
			if (!runtimePerBotPerVm.containsKey(vm)) {
				Map<String, WienerProcessModel> runtimePerBot = new HashMap<>();
				runtimePerBotPerVm.put(vm, runtimePerBot);
			}
		}

		Set<String> bots = new HashSet<>();
		for (Task task : tasks) {
			bots.add(task.getName());
		}
		for (String bot : bots) {
			for (Vm vm : vms) {
				runtimePerBotPerVm.get(vm).put(bot, new WienerProcessModel(bot, vm.getId()));
			}
		}

		todo = new HashSet<>(tasks);
	}

	@Override
	public Task getNextTask(Vm vm) {
		boolean replicate = false;

		Map<String, Queue<Task>> b_ready = readyTasksPerBot;
		Map<String, Set<Task>> b_run = runningTasksPerBot;

		Map<String, Queue<Task>> b_select = b_ready;
		if (b_ready.isEmpty()) {
			b_select = new HashMap<>();
			for (Entry<String, Set<Task>> e : b_run.entrySet()) {
				Queue<Task> tasks = new LinkedList<>(e.getValue());
				b_select.put(e.getKey(), tasks);
			}
			replicate = true;
		}

		Set<Vm> m = runtimePerBotPerVm.keySet();
		Queue<Task> b_min = null;
		double s_min = Double.MAX_VALUE;
		for (Entry<String, Queue<Task>> b_i : b_select.entrySet()) {
			double e_j = runtimePerBotPerVm.get(vm).get(b_i.getKey()).getEstimate(CloudSim.clock(), replicate ? 0.5 : alpha);
			if (e_j == 0) {
				if (!replicate) {
					b_min = b_i.getValue();
					break;
				}
				e_j = Double.MAX_VALUE;
			}

			double e_min = Double.MAX_VALUE;
			double e_sum = e_j;
			int e_num = 1;
			for (Vm k : m) {
				double e_k = runtimePerBotPerVm.get(k).get(b_i.getKey()).getEstimate(CloudSim.clock(), replicate ? 0.5 : alpha);
				if (k.equals(vm) || e_k == 0)
					continue;
				if (e_k < e_min) {
					e_min = e_k;
				}
				e_sum += e_k;
				e_num++;
			}

			double s = (e_j - e_min) / (e_sum / e_num);
			if (s < s_min) {
				s_min = s;
				b_min = b_i.getValue();
			}
		}

		if (b_min != null && !b_min.isEmpty()) {
			Task task = b_min.remove();

			if (replicate) {
				task = new Task(task);
				task.setSpeculativeCopy(true);
				replicas.add(task);
			} else {
				if (!runningTasksPerBot.containsKey(task.getName())) {
					Set<Task> s = new HashSet<>();
					runningTasksPerBot.put(task.getName(), s);
				}
				runningTasksPerBot.get(task.getName()).add(task);

				if (readyTasksPerBot.get(task.getName()).isEmpty()) {
					readyTasksPerBot.remove(task.getName());
				}

			}

			return task;
		}

		return null;
	}

	@Override
	public void taskReady(Task task) {
		if (!readyTasksPerBot.containsKey(task.getName())) {
			Queue<Task> q = new LinkedList<>();
			readyTasksPerBot.put(task.getName(), q);
		}
		readyTasksPerBot.get(task.getName()).add(task);
	}

	@Override
	public boolean tasksRemaining() {
		return (!signalFinished() && (!readyTasksPerBot.isEmpty() || replicas.size() < rho));
	}

	@Override
	public boolean signalFinished() {
		return todo.isEmpty();
	}

	@Override
	public void taskSucceeded(Task task, Vm vm) {
		double runtime = task.getFinishTime() - task.getExecStartTime();
		runtimePerBotPerVm.get(vm).get(task.getName()).addRuntime(task.getFinishTime(), runtime);
		todo.remove(task);

		taskFinished(task);
	}

	@Override
	public void taskFailed(Task task, Vm vm) {
		taskFinished(task);
	}

	private void taskFinished(Task task) {
		// if (task.isSpeculativeCopy()) {
		while (replicas.contains(task))
			replicas.remove(task);
		// } else {
		if (runningTasksPerBot.containsKey(task.getName())) {
			runningTasksPerBot.get(task.getName()).remove(task);
			if (runningTasksPerBot.get(task.getName()).isEmpty())
				runningTasksPerBot.remove(task.getName());
		}
	}

	@Override
	public void terminate() {
		if (printEstimatesVsRuntimes) {
			for (Map<String, WienerProcessModel> m : runtimePerBotPerVm.values()) {
				for (WienerProcessModel w : m.values()) {
					w.printEstimatesVsRuntimes();
				}
			}
		}
	}

}
