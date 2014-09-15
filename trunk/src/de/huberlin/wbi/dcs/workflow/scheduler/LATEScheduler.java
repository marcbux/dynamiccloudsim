package de.huberlin.wbi.dcs.workflow.scheduler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;

import de.huberlin.wbi.dcs.examples.Parameters;
import de.huberlin.wbi.dcs.workflow.Task;

public class LATEScheduler extends AbstractWorkflowScheduler {

	public class TaskProgressRateComparator implements Comparator<Task> {

		@Override
		public int compare(Task task1, Task task2) {
			return Double.compare(progressRates.get(task1),
					progressRates.get(task2));
		}

	}

	public class TaskEstimatedTimeToCompletionComparator implements
			Comparator<Task> {

		@Override
		public int compare(Task task1, Task task2) {
			return Double.compare(estimatedTimesToCompletion.get(task1),
					estimatedTimesToCompletion.get(task2));
		}

	}

	protected Queue<Task> taskQueue;

	private int taskSlotsPerVm;

	// a node is slow if the sum of progress scores for all succeeded and
	// in-progress tasks on the node is below this threshold
	// speculative tasks are not executed on slow nodes
	// default: 25th percentile of node progress rates
	protected final double slowNodeThreshold = 0.25;
	protected double currentSlowNodeThreshold;

	// A threshold that a task's progress rate is compared with to determine
	// whether it is slow enough to be speculated upon
	// default: 25th percentile of task progress rates
	protected final double slowTaskThreshold = 0.25;
	protected double currentSlowTaskThreshold;

	// a cap on the number of speculative tasks that can be running at once
	// (given as a percentage of task slots)
	// default: 10% of available task slots
	protected final double speculativeCap = 0.1;
	protected int speculativeCapAbs;

	protected Map<Vm, Double> nSucceededTasksPerVm;

	private Map<Task, Double> progressScores;
	private Map<Task, Double> timesTaskHasBeenRunning;
	private Map<Task, Double> progressRates;
	private Map<Task, Double> estimatedTimesToCompletion;

	// two collections of tasks, which are currently running;
	// note that the second collections is a subset of the first collection
	protected Set<Task> tasks;
	protected Set<Task> speculativeTasks;

	public LATEScheduler(String name, int taskSlotsPerVm) throws Exception {
		super(name, taskSlotsPerVm);
		this.taskSlotsPerVm = taskSlotsPerVm;
		nSucceededTasksPerVm = new HashMap<>();
		taskQueue = new LinkedList<>();
		tasks = new HashSet<>();
		speculativeTasks = new HashSet<>();
		progressScores = new HashMap<>();
		timesTaskHasBeenRunning = new HashMap<>();
		progressRates = new HashMap<>();
		estimatedTimesToCompletion = new HashMap<>();
	}

	@Override
	public void reschedule(Collection<Task> tasks, Collection<Vm> vms) {
		speculativeCapAbs = (int) (vms.size() * taskSlotsPerVm * speculativeCap + 0.5);
		for (Vm vm : vms) {
			if (!nSucceededTasksPerVm.containsKey(vm)) {
				nSucceededTasksPerVm.put(vm, 0d);
			}
		}
		for (Task task : tasks) {
			progressScores.put(task, 0d);
		}
	}

	public class VmSumOfProgressScoresComparator implements Comparator<Vm> {

		Map<Integer, Double> vmIdToSumOfProgressScores;

		public VmSumOfProgressScoresComparator(
				Map<Integer, Double> vmIdToSumOfProgressScores) {
			this.vmIdToSumOfProgressScores = vmIdToSumOfProgressScores;
		}

		@Override
		public int compare(Vm vm1, Vm vm2) {
			return Double.compare(vmIdToSumOfProgressScores.get(vm1.getId()),
					vmIdToSumOfProgressScores.get(vm2.getId()));
		}

	}

	@Override
	public void taskReady(Task task) {
		taskQueue.add(task);

	}

	@Override
	public boolean tasksRemaining() {
		return !taskQueue.isEmpty()
				|| (tasks.size() > speculativeTasks.size() && speculativeTasks
						.size() < speculativeCapAbs);
	}

	@Override
	public void taskSucceeded(Task task, Vm vm) {
		tasks.remove(task);
		speculativeTasks.remove(task);
		nSucceededTasksPerVm.put(vm, nSucceededTasksPerVm.get(vm)
				+ progressScores.get(task));
	}

	private void computeProgressScore(Task task) {
		double actualProgressScore = (double) (task.getCloudletFinishedSoFar())
				/ (double) (task.getCloudletLength());
		// the distortion is higher if task is really close to finish or just
		// started recently
		double distortionIntensity = 1d - Math
				.abs(1d - actualProgressScore * 2d);
		double distortion = Parameters.numGen.nextGaussian()
				* Parameters.distortionCV * distortionIntensity;
		double perceivedProgressScore = actualProgressScore + distortion;
		progressScores.put(task,
				(perceivedProgressScore > 1) ? 0.99
						: ((perceivedProgressScore < 0) ? 0.01
								: perceivedProgressScore));
	}

	private void computeProgressRate(Task task) {
		computeProgressScore(task);
		timesTaskHasBeenRunning.put(task,
				CloudSim.clock() - task.getExecStartTime());
		progressRates.put(
				task,
				(timesTaskHasBeenRunning.get(task) == 0) ? Double.MAX_VALUE
						: progressScores.get(task)
								/ timesTaskHasBeenRunning.get(task));
	}

	public void computeEstimatedTimeToCompletion(Task task) {
		computeProgressRate(task);
		estimatedTimesToCompletion.put(
				task,
				(progressRates.get(task) == 0) ? Double.MAX_VALUE
						: (1d - progressScores.get(task))
								/ progressRates.get(task));
	}

	@Override
	public void taskFailed(Task task, Vm vm) {
		if (task.isSpeculativeCopy()) {
			speculativeTasks.remove(task);
		} else {
			tasks.remove(task);
		}
		nSucceededTasksPerVm.put(vm, nSucceededTasksPerVm.get(vm)
				+ progressScores.get(task));
	}

	@Override
	public void terminate() {
	}

	@Override
	public Task getNextTask(Vm vm) {
		// compute the sum of progress scores for all vms
		Map<Integer, Double> vmIdToSumOfProgressScores = new HashMap<>();
		for (Vm runningVm : nSucceededTasksPerVm.keySet()) {
			vmIdToSumOfProgressScores.put(runningVm.getId(),
					(double) nSucceededTasksPerVm.get(runningVm));
		}

		for (Task t : tasks) {
			computeEstimatedTimeToCompletion(t);
			vmIdToSumOfProgressScores.put(
					t.getVmId(),
					vmIdToSumOfProgressScores.get(t.getVmId())
							+ progressScores.get(t));
		}

		// compute the quantiles of task and node slowness
		List<Vm> runningVms = new ArrayList<>(nSucceededTasksPerVm.keySet());
		Collections.sort(runningVms, new VmSumOfProgressScoresComparator(
				vmIdToSumOfProgressScores));
		int quantileIndex = (int) (runningVms.size() * slowNodeThreshold - 0.5);
		currentSlowNodeThreshold = vmIdToSumOfProgressScores.get(runningVms
				.get(quantileIndex).getId());

		List<Task> runningTasks = new ArrayList<>(tasks);
		Collections.sort(runningTasks, new TaskProgressRateComparator());
		quantileIndex = (int) (runningTasks.size() * slowTaskThreshold - 0.5);
		currentSlowTaskThreshold = (runningTasks.size() > 0) ? progressRates
				.get(runningTasks.get(quantileIndex)) : -1;

		// determine a candidate for speculative execution
		List<Task> candidates = new LinkedList<>();
		for (Task candidate : tasks) {
			if (!speculativeTasks.contains(candidate)
					&& progressRates.get(candidate) < currentSlowTaskThreshold) {
				System.out.println(progressRates.get(candidate) + " "
						+ currentSlowTaskThreshold);
				candidates.add(candidate);
			}
		}
		Collections.sort(candidates,
				new TaskEstimatedTimeToCompletionComparator());

		// check whether a task is to be executed speculatively or regularly
		if (candidates.size() > 0
				&& tasks.size() > speculativeTasks.size()
				&& speculativeTasks.size() < speculativeCapAbs
				&& vmIdToSumOfProgressScores.get(vm.getId()) > currentSlowNodeThreshold) {
			Task task = candidates.get(candidates.size() - 1);
			Task speculativeTask = new Task(task);
			speculativeTask.setSpeculativeCopy(true);
			speculativeTasks.add(speculativeTask);
			return speculativeTask;
		} else if (!taskQueue.isEmpty()) {
			Task task = taskQueue.remove();
			tasks.add(task);
			return task;
		}
		return null;
	}

}
