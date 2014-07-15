package de.huberlin.wbi.dcs.workflow.scheduler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.SimEvent;

import de.huberlin.wbi.dcs.examples.Parameters;
import de.huberlin.wbi.dcs.workflow.DataDependency;
import de.huberlin.wbi.dcs.workflow.Task;
import de.huberlin.wbi.dcs.workflow.TaskNumberOfSuccessorsComparator;
import de.huberlin.wbi.dcs.workflow.Workflow;

public class C3 extends WorkflowScheduler {
	
	boolean doSample = false;
	
	Random numGen;
	
	private double conservatismWeight = 3;
	private double outlookWeight = 2;
//	private int nClones = 2;
	
	// one job queue for each task category, identified by name
	protected Map<String, ArrayList<Task>> queuePerTaskCategory;
	protected Map<String, Integer> remainingTasksPerCategory;
	
	// for each task category, remember two figures:
	// (1) how many task instances of this category have been successfully executed
	// (2) how much time has been spent in total for task instances of this category
	protected Map<String, Integer> taskExecutionsPerTaskCategory;
	protected Map<String, Double> timeSpentPerTaskCategory;
	
	// for each VM and each task category, remember the last execution time
	protected Map<Integer, Map<String, Double>> lastRuntimePerTaskCategoryAndVmId;
	
	protected Map<Integer, Task> speculativeTasks;
	
	/* 
	 * The basic concept of this scheduler is as follows:
	 * If a task slot is available on vm x:
	 *   (1) compute how much each task category contributes to overall remaining execution time (as inferred from queuePerTaskCategory and timeSpentPerTaskCategory / taskExecutionsPerTaskCategory), e.g.
	 *     preprocess 	10% 0.1
	 *     align		60% 0.6
	 *     VC			30% 0.3
	 *   (2) compute how well-suited this vm is to execute each task category, in comparison to all the other vms (inferred from lastRuntimePerTaskCategoryAndVm), e.g.
	 *     preprocess	50% 0.5      (vs 10%, 30% and 10% on other vms)
	 *     align		20% 0.2
	 *     VC			40% 0.4
	 *     ^ maybe the slowest for a category ("supertrumpf") should have a score like 1% somehow, but maybe not...
	 *     
	 *     
	 *   (3) compute the product
	 *     preprocess 	0.05
	 *     align		0.12
	 *     VC			0.12
	 * 
	 *   (4) normalize
	 *     preprocess 	0.18
	 *     align		0.41
	 *     VC			0.41
	 *     
	 *   (5) either sample from this range or take a task instance from the category with the highest score
	 */
	
	public C3(String name, int taskSlotsPerVm) throws Exception {
		super(name, taskSlotsPerVm);
		queuePerTaskCategory = new HashMap<String, ArrayList<Task>>();
		remainingTasksPerCategory = new HashMap<String, Integer>();
		taskExecutionsPerTaskCategory = new HashMap<String, Integer>();
		timeSpentPerTaskCategory = new HashMap<String, Double>();
		lastRuntimePerTaskCategoryAndVmId = new HashMap<Integer, Map<String, Double>>();
		numGen = Parameters.numGen;
	}
	
	@Override
	protected void registerVms() {
		super.registerVms();
		for (int id : vms.keySet()) {
			lastRuntimePerTaskCategoryAndVmId.put(id, new HashMap<String, Double>());
		}
	}
	
	protected void registerTasks() {
		for (Workflow workflow : workflows) {
			for (Task task : workflow.getSortedTasks()) {
				if (!remainingTasksPerCategory.containsKey(task.getName())) {
					remainingTasksPerCategory.put(task.getName(), 0);
					queuePerTaskCategory.put(task.getName(), new ArrayList<Task>());
					taskExecutionsPerTaskCategory.put(task.getName(), 0);
					timeSpentPerTaskCategory.put(task.getName(), 0d);
					for (int id : vms.keySet()) {
						lastRuntimePerTaskCategoryAndVmId.get(id).put(task.getName(), 0d);
					}
				}
				
				remainingTasksPerCategory.put(task.getName(), remainingTasksPerCategory.get(task.getName()) + 1);
				if (task.readyToExecute()) {
					queuePerTaskCategory.get(task.getName()).add(task);
				}
			}
		}
		sortQueuesByNumberOfSuccessors();
	}
	
	protected void submitTasks() {
		while (!idleTaskSlots.isEmpty()) {
			// (1) if all taskQueues are empty, there's currently nothing to do
			if (!tasksInQueue()) {
				break;
			}
			
			// (2) Curiosity is encouraged: if there is a task category, which has not been executed yet by this Vm, execute it
			Vm vm = idleTaskSlots.remove();
			boolean submitted = false;
			for (String cat : queuePerTaskCategory.keySet()) {
				if (queuePerTaskCategory.get(cat).size() > 0 && lastRuntimePerTaskCategoryAndVmId.get(vm.getId()).get(cat) == 0d) {
					submitTask(queuePerTaskCategory.get(cat).remove(0), vm);
					submitted = true;
					break;
				}
			}
			if (submitted) {
				continue;
			}
			
			// (3) Otherwise, commence with default routine
			//     (a) compute the share each task category with ready-to-execute tasks contributes to overall runtime
			Map<String, Double> shareOfRemainingRuntimePerCategory = new HashMap<String, Double>();
			double sumOfShares = 0d;
			for (String cat : queuePerTaskCategory.keySet()) {
				if (queuePerTaskCategory.get(cat).size() < 1) {
					continue;
				}
				int taskExecutions = taskExecutionsPerTaskCategory.get(cat); // this value can't be zero since if it were zero, the task would have been selected for execution in (2)
				double timeSpent = timeSpentPerTaskCategory.get(cat);
				int remainingTasks = remainingTasksPerCategory.get(cat);
				double shareOfRemainingRuntime = remainingTasks * (timeSpent / taskExecutions);
				sumOfShares += shareOfRemainingRuntime;
				shareOfRemainingRuntimePerCategory.put(cat, shareOfRemainingRuntime);
			}
			for (String cat : shareOfRemainingRuntimePerCategory.keySet()) {
				shareOfRemainingRuntimePerCategory.put(cat, shareOfRemainingRuntimePerCategory.get(cat) / sumOfShares);
			}
			
			//     (b) compute the suitability of each vm to execute each task
			Map<String, Double> suitabilityOfThisVmByCategory = new HashMap<String, Double>();
			double sumOfSuitabilities = 0d;
			for (String cat : shareOfRemainingRuntimePerCategory.keySet()) {
				Map<Integer, Double> runtimeForCategoryByVm = new HashMap<Integer, Double>();
				double sum = 0d;
				for (Integer vmId : vms.keySet()) {
					double lastRuntime = lastRuntimePerTaskCategoryAndVmId.get(vmId).get(cat);
					sum += lastRuntime;
					if (lastRuntime > 0d) {
						runtimeForCategoryByVm.put(vmId, lastRuntime);
					}
				}
				// normalize runtimes to sum up to 1
				for (Integer vmId : runtimeForCategoryByVm.keySet()) {
					runtimeForCategoryByVm.put(vmId, runtimeForCategoryByVm.get(vmId) / sum);
				}
				
				// suitability = inverse runtime
				Map<Integer, Double> suitabilityForCategoryByVm = new HashMap<Integer, Double>();
				sum = 0d;
				for (Integer vmId : runtimeForCategoryByVm.keySet()) {
					double suitability = 1d / runtimeForCategoryByVm.get(vmId);
					sum += suitability;
					suitabilityForCategoryByVm.put(vmId, suitability);
				}
				// normalize again...
				for (Integer vmId : suitabilityForCategoryByVm.keySet()) {
					suitabilityForCategoryByVm.put(vmId, suitabilityForCategoryByVm.get(vmId) / sum);
				}
				
				double suitability = suitabilityForCategoryByVm.get(vm.getId());
				sumOfSuitabilities += suitability;
				suitabilityOfThisVmByCategory.put(cat, suitability);
			}
			for (String cat : suitabilityOfThisVmByCategory.keySet()) {
				suitabilityOfThisVmByCategory.put(cat, suitabilityOfThisVmByCategory.get(cat) / sumOfSuitabilities);
			}
			
			//     (c) compute the (normalized) product of necessity to execute a given task (due to large runtime of this category) and suitability of this vm
			Map<String, Double> likelihoofOfExecutionPerCategory = new HashMap<String, Double>();
			double sumOfLikelihoods = 0d;
			for (String cat : shareOfRemainingRuntimePerCategory.keySet()) {
				double likelihood = Math.pow(shareOfRemainingRuntimePerCategory.get(cat), outlookWeight) * Math.pow(suitabilityOfThisVmByCategory.get(cat), conservatismWeight);
				sumOfLikelihoods += likelihood;
				likelihoofOfExecutionPerCategory.put(cat,  likelihood);
			}
			for (String cat : likelihoofOfExecutionPerCategory.keySet()) {
				likelihoofOfExecutionPerCategory.put(cat, likelihoofOfExecutionPerCategory.get(cat) / sumOfLikelihoods);
			}
			
			//     (d) sample and submit task
			if (doSample) {
				Iterator<String> catIt = likelihoofOfExecutionPerCategory.keySet().iterator();
				String cat = catIt.next();
				double sample = numGen.nextDouble();
				sumOfLikelihoods = 0d;
				while (sample > (sumOfLikelihoods += likelihoofOfExecutionPerCategory.get(cat)))  {
					cat = catIt.next();
				}
				submitTask(queuePerTaskCategory.get(cat).remove(0), vm);
			} else {
				double maxLikelihood = 0;
				String maxCat = null;
				for (String cat : likelihoofOfExecutionPerCategory.keySet()) {
					if (likelihoofOfExecutionPerCategory.get(cat) > maxLikelihood) {
						maxLikelihood = likelihoofOfExecutionPerCategory.get(cat);
						maxCat = cat;
					}
				}
				submitTask(queuePerTaskCategory.get(maxCat).remove(0), vm);
			}
			
		}
	}
	
	private boolean tasksInQueue() {
		boolean tasksInQueue = false;
		for (ArrayList<Task> queue : queuePerTaskCategory.values()) {
			if (queue.size() > 0) {
				tasksInQueue = true;
				break;
			}
		}
		return tasksInQueue;
	}
	
	@Override
	protected void submitCloudlets() {
		registerVms();
		registerTasks();
		submitTasks();
	}
	
	@Override
	protected void submitTask(Task task, Vm vm) {
		remainingTasksPerCategory.put(task.getName(), remainingTasksPerCategory.get(task.getName()) - 1);
		super.submitTask(task, vm);
	}

	@Override
	protected void processCloudletReturn(SimEvent ev) {
		Task task = (Task) ev.getData();
		Vm vm = vms.get(task.getVmId());
		idleTaskSlots.add(vm);
		if (task.getCloudletStatus() == Cloudlet.SUCCESS) {
			Log.printLine(CloudSim.clock() + ": " + getName() + ": VM # "
					+ vm.getId() + " completed Task # " + task.getCloudletId() + " \"" + task.getName() + " " + task.getParams() + " \"");
			
			
			double runtime = task.getFinishTime() - task.getExecStartTime();
			taskExecutionsPerTaskCategory.put(task.getName(), taskExecutionsPerTaskCategory.get(task.getName()) + 1);
			timeSpentPerTaskCategory.put(task.getName(), timeSpentPerTaskCategory.get(task.getName()) + runtime);
			lastRuntimePerTaskCategoryAndVmId.get(vm.getId()).put(task.getName(), runtime);
			
			for (DataDependency outgoingEdge : task.getWorkflow().getGraph().getOutEdges(task)) {
				Task child = task.getWorkflow().getGraph().getDest(outgoingEdge);
				child.decNDataDependencies();
				if (child.readyToExecute()) {
					queuePerTaskCategory.get(child.getName()).add(child);
				}
			}
		} else {
			Log.printLine(CloudSim.clock() + ": " + getName() + ": VM # " + vm.getId() + " encountered an error with Task # " + task.getCloudletId() + " \"" + task.getName() + " " + task.getParams() + " \"");
			resetTask(task);
			queuePerTaskCategory.get(task.getName()).add(task);
			remainingTasksPerCategory.put(task.getName(), remainingTasksPerCategory.get(task.getName()) + 1);
		}

		// put this in again later (just taken out for performance reasons)
//		sortQueuesByNumberOfSuccessors();

		if (tasksInQueue()) {
			submitTasks();
		} else if (idleTaskSlots.size() == getVmsCreatedList().size() * getTaskSlotsPerVm()) {
			Log.printLine(CloudSim.clock() + ": " + getName()
					+ ": All Tasks executed. Finishing...");
			clearDatacenters();
			finishExecution();
		}
	}
	
	private void sortQueuesByNumberOfSuccessors() {
		for (String cat : queuePerTaskCategory.keySet()) {
			Collections.sort(queuePerTaskCategory.get(cat), new TaskNumberOfSuccessorsComparator());
		}
	}

}
