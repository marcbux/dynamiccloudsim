package de.huberlin.wbi.dcs.workflow.scheduler;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Random;

import org.cloudbus.cloudsim.Vm;

import de.huberlin.wbi.dcs.examples.Parameters;
import de.huberlin.wbi.dcs.workflow.Task;

public class C3 extends AbstractWorkflowScheduler {

	boolean doSample = true;

	Random numGen;

	private double conservatismWeight = 3;
	private double outlookWeight = 2;
	// private int nClones = 2;

	// one job queue for each task category, identified by name
	protected Map<String, Queue<Task>> queuePerTaskCategory;
	protected Map<String, Integer> remainingTasksPerCategory;

	// for each task category, remember two figures:
	// (1) how many task instances of this category have been successfully
	// executed
	// (2) how much time has been spent in total for task instances of this
	// category
	protected Map<String, Integer> taskExecutionsPerTaskCategory;
	protected Map<String, Double> timeSpentPerTaskCategory;

	// for each VM and each task category, remember the last execution time
	protected Map<Vm, Map<String, Double>> lastRuntimePerTaskCategoryAndVm;

	// protected Map<Integer, Task> speculativeTasks;

	/*
	 * The basic concept of this scheduler is as follows: If a task slot is
	 * available on vm x: (1) compute how much each task category contributes to
	 * overall remaining execution time (as inferred from queuePerTaskCategory
	 * and timeSpentPerTaskCategory / taskExecutionsPerTaskCategory), e.g.
	 * preprocess 10% 0.1 align 60% 0.6 VC 30% 0.3 (2) compute how well-suited
	 * this vm is to execute each task category, in comparison to all the other
	 * vms (inferred from lastRuntimePerTaskCategoryAndVm), e.g. preprocess 50%
	 * 0.5 (vs 10%, 30% and 10% on other vms) align 20% 0.2 VC 40% 0.4 ^ maybe
	 * the slowest for a category ("supertrumpf") should have a score like 1%
	 * somehow, but maybe not...
	 * 
	 * 
	 * (3) compute the product preprocess 0.05 align 0.12 VC 0.12
	 * 
	 * (4) normalize preprocess 0.18 align 0.41 VC 0.41
	 * 
	 * (5) either sample from this range or take a task instance from the
	 * category with the highest score
	 */

	public C3(String name, int taskSlotsPerVm) throws Exception {
		super(name, taskSlotsPerVm);
		queuePerTaskCategory = new HashMap<>();
		remainingTasksPerCategory = new HashMap<>();
		taskExecutionsPerTaskCategory = new HashMap<>();
		timeSpentPerTaskCategory = new HashMap<>();
		lastRuntimePerTaskCategoryAndVm = new HashMap<>();
		numGen = Parameters.numGen;
	}

	@Override
	public void reschedule(Collection<Task> tasks, Collection<Vm> vms) {
		for (Vm vm : vms) {
			if (!lastRuntimePerTaskCategoryAndVm.containsKey(vm)) {
				Map<String, Double> m = new HashMap<>();
				lastRuntimePerTaskCategoryAndVm.put(vm, m);
			}
		}
		for (Task task : tasks) {
			if (!remainingTasksPerCategory.containsKey(task.getName())) {
				remainingTasksPerCategory.put(task.getName(), 0);
				Queue<Task> q = new LinkedList<>();
				queuePerTaskCategory.put(task.getName(), q);
				taskExecutionsPerTaskCategory.put(task.getName(), 0);
				timeSpentPerTaskCategory.put(task.getName(), 0d);
				for (Vm vm : vms) {
					lastRuntimePerTaskCategoryAndVm.get(vm).put(task.getName(),
							0d);
				}
			}
			remainingTasksPerCategory.put(task.getName(),
					remainingTasksPerCategory.get(task.getName()) + 1);
		}
	}

	@Override
	public Task getNextTask(Vm vm) {
		// (2) Curiosity is encouraged: if there is a task category, which
		// has not been executed yet by this Vm, execute it
		for (String cat : queuePerTaskCategory.keySet()) {
			if (queuePerTaskCategory.get(cat).size() > 0
					&& lastRuntimePerTaskCategoryAndVm.get(vm).get(cat) == 0d) {
				Task task = queuePerTaskCategory.get(cat).remove();
				remainingTasksPerCategory.put(task.getName(),
						remainingTasksPerCategory.get(task.getName()) - 1);
				return task;
			}
		}

		// (3) Otherwise, commence with default routine
		// (a) compute the share each task category with ready-to-execute
		// tasks contributes to overall runtime
		Map<String, Double> shareOfRemainingRuntimePerCategory = new HashMap<>();
		double sumOfShares = 0d;
		for (String cat : queuePerTaskCategory.keySet()) {
			if (queuePerTaskCategory.get(cat).size() < 1) {
				continue;
			}
			// this vaue can't be zero since if it were zero, the task would
			// have been selected for execution in (2)
			int taskExecutions = taskExecutionsPerTaskCategory.get(cat);
			double timeSpent = timeSpentPerTaskCategory.get(cat);
			int remainingTasks = remainingTasksPerCategory.get(cat);
			double shareOfRemainingRuntime = remainingTasks
					* (timeSpent / taskExecutions);
			sumOfShares += shareOfRemainingRuntime;
			shareOfRemainingRuntimePerCategory
					.put(cat, shareOfRemainingRuntime);
		}
		for (String cat : shareOfRemainingRuntimePerCategory.keySet()) {
			shareOfRemainingRuntimePerCategory.put(cat,
					shareOfRemainingRuntimePerCategory.get(cat) / sumOfShares);
		}

		// (b) compute the suitability of each vm to execute each task
		Map<String, Double> suitabilityOfThisVmByCategory = new HashMap<>();
		double sumOfSuitabilities = 0d;
		for (String cat : shareOfRemainingRuntimePerCategory.keySet()) {
			Map<Vm, Double> runtimeForCategoryByVm = new HashMap<>();
			double sum = 0d;
			for (Vm runningVm : lastRuntimePerTaskCategoryAndVm.keySet()) {
				double lastRuntime = lastRuntimePerTaskCategoryAndVm.get(
						runningVm).get(cat);
				sum += lastRuntime;
				if (lastRuntime > 0d) {
					runtimeForCategoryByVm.put(runningVm, lastRuntime);
				}
			}
			// normalize runtimes to sum up to 1
			for (Vm runningVm : runtimeForCategoryByVm.keySet()) {
				runtimeForCategoryByVm.put(runningVm,
						runtimeForCategoryByVm.get(runningVm) / sum);
			}

			// suitability = inverse runtime
			Map<Vm, Double> suitabilityForCategoryByVm = new HashMap<>();
			sum = 0d;
			for (Vm runningVm : runtimeForCategoryByVm.keySet()) {
				double suitability = 1d / runtimeForCategoryByVm.get(runningVm);
				sum += suitability;
				suitabilityForCategoryByVm.put(runningVm, suitability);
			}
			// normalize again...
			for (Vm runningVm : suitabilityForCategoryByVm.keySet()) {
				suitabilityForCategoryByVm.put(runningVm,
						suitabilityForCategoryByVm.get(runningVm) / sum);
			}

			double suitability = suitabilityForCategoryByVm.get(vm);
			sumOfSuitabilities += suitability;
			suitabilityOfThisVmByCategory.put(cat, suitability);
		}
		for (String cat : suitabilityOfThisVmByCategory.keySet()) {
			suitabilityOfThisVmByCategory
					.put(cat, suitabilityOfThisVmByCategory.get(cat)
							/ sumOfSuitabilities);
		}

		// (c) compute the (normalized) product of necessity to execute a
		// given task (due to large runtime of this category) and
		// suitability of this vm
		Map<String, Double> likelihoofOfExecutionPerCategory = new HashMap<>();
		double sumOfLikelihoods = 0d;
		for (String cat : shareOfRemainingRuntimePerCategory.keySet()) {
			double likelihood = Math.pow(
					shareOfRemainingRuntimePerCategory.get(cat), outlookWeight)
					* Math.pow(suitabilityOfThisVmByCategory.get(cat),
							conservatismWeight);
			sumOfLikelihoods += likelihood;
			likelihoofOfExecutionPerCategory.put(cat, likelihood);
		}
		for (String cat : likelihoofOfExecutionPerCategory.keySet()) {
			likelihoofOfExecutionPerCategory.put(cat,
					likelihoofOfExecutionPerCategory.get(cat)
							/ sumOfLikelihoods);
		}

		// (d) sample and submit task
		if (doSample) {
			Iterator<String> catIt = likelihoofOfExecutionPerCategory.keySet()
					.iterator();
			String cat = catIt.next();
			double sample = numGen.nextDouble();
			sumOfLikelihoods = 0d;
			while (sample > (sumOfLikelihoods += likelihoofOfExecutionPerCategory
					.get(cat))) {
				cat = catIt.next();
			}
			Task task = queuePerTaskCategory.get(cat).remove();
			remainingTasksPerCategory.put(task.getName(),
					remainingTasksPerCategory.get(task.getName()) - 1);
			return task;
		}
		double maxLikelihood = 0;
		String maxCat = null;
		for (String cat : likelihoofOfExecutionPerCategory.keySet()) {
			if (likelihoofOfExecutionPerCategory.get(cat) > maxLikelihood) {
				maxLikelihood = likelihoofOfExecutionPerCategory.get(cat);
				maxCat = cat;
			}
		}
		Task task = queuePerTaskCategory.get(maxCat).remove();
		remainingTasksPerCategory.put(task.getName(),
				remainingTasksPerCategory.get(task.getName()) - 1);
		return task;
	}

	@Override
	public void taskFailed(Task task, Vm vm) {
		remainingTasksPerCategory.put(task.getName(),
				remainingTasksPerCategory.get(task.getName()) + 1);
	}

	@Override
	public void taskReady(Task task) {
		queuePerTaskCategory.get(task.getName()).add(task);
		// sortQueuesByNumberOfSuccessors();
	}

	@Override
	public boolean tasksRemaining() {
		boolean tasksInQueue = false;
		for (Queue<Task> queue : queuePerTaskCategory.values()) {
			if (queue.size() > 0) {
				tasksInQueue = true;
				break;
			}
		}
		return tasksInQueue;
	}

	@Override
	public void taskSucceeded(Task task, Vm vm) {
		double runtime = task.getFinishTime() - task.getExecStartTime();
		taskExecutionsPerTaskCategory.put(task.getName(),
				taskExecutionsPerTaskCategory.get(task.getName()) + 1);
		timeSpentPerTaskCategory.put(task.getName(),
				timeSpentPerTaskCategory.get(task.getName()) + runtime);
		lastRuntimePerTaskCategoryAndVm.get(vm).put(task.getName(), runtime);
	}

	@Override
	public void terminate() {
	}

	// private void sortQueuesByNumberOfSuccessors() {
	// for (String cat : queuePerTaskCategory.keySet()) {
	// Collections.sort(queuePerTaskCategory.get(cat),
	// new TaskNumberOfSuccessorsComparator());
	// }
	// }

}
