package de.huberlin.dcs.workflow.io;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.cloudbus.cloudsim.ParameterException;
import org.cloudbus.cloudsim.UtilizationModel;
import org.cloudbus.cloudsim.UtilizationModelFull;

import de.huberlin.dcs.workflow.Task;
import de.huberlin.dcs.workflow.Workflow;

public class AlignmentTraceFileReader {
	
	public static int cloudletId = 1;
	
	File log;
	
	Map<String, org.cloudbus.cloudsim.File> fileNameToFile;
	Map<String, Integer> fileNameToProducingTaskId;
	Map<String, List<Integer>> fileNameToConsumingTaskIds;
	Map<Integer, Task> taskIdToTask;
	
	// instead of full file paths, use filenames as unique identifier
	// to determine data dependencies
	boolean fileNames;
	
	// add kernel time to user time when determining the runtime of a process
	boolean kernelTime;
	
	// omit upload of intermediate files by specifiyng how output data has to look like
	String outputFileRegex;
	
	public AlignmentTraceFileReader(File log, boolean fileNames, boolean kernelTime, String outputFileRegex) {
		this.log = log;
		fileNameToFile = new HashMap<String, org.cloudbus.cloudsim.File>();
		fileNameToProducingTaskId = new HashMap<String, Integer>();
		fileNameToConsumingTaskIds = new HashMap<String, List<Integer>>();
		taskIdToTask = new HashMap<Integer, Task>();
		this.fileNames = fileNames;
		this.kernelTime = kernelTime;
		if (outputFileRegex == null) {
			this.outputFileRegex = ".*";
		} else {
			this.outputFileRegex = outputFileRegex;
		}
	}
	
	public Workflow parseLogFile(int userId) {
		Workflow workflow = new Workflow();
		
		try {
			BufferedReader logfile = new BufferedReader(new FileReader(log));
			
			String line = logfile.readLine();
			String[] splitLine;
			UtilizationModel utilizationModel = new UtilizationModelFull();
			while (line != null) {
				splitLine = line.split("\t");
				String name = splitLine[2];
				String params = "";
				int timeInMs = 0;
				int inputSize = 0;
				int outputSize = 0;
				
				do {
					splitLine = line.split("\t");
					String fileName = splitLine[5];
					if (fileNames) {
						fileName = fileName.substring(Math.max(0, fileName.lastIndexOf('/') + 1));
					}
					int fileSize = Integer.parseInt(splitLine[4]) / 1000;
					fileSize = (fileSize > 0) ? fileSize : 1;
					
					if (!fileNameToFile.containsKey(fileName)) {
						fileNameToFile.put(fileName, new org.cloudbus.cloudsim.File(fileName, fileSize));
						fileNameToConsumingTaskIds.put(fileName, new ArrayList<Integer>());
					}
					fileNameToConsumingTaskIds.get(fileName).add(cloudletId);
					inputSize += fileSize;
				} while ((line = logfile.readLine()).contains("input-file"));
				
				for (int i = 0; i < 2; i++) {
					splitLine = logfile.readLine().split("\t");
					if (kernelTime || i < 1) {
						timeInMs += (int) (Float.parseFloat(splitLine[4]) * 1000);
					}
				}
				
				while ((line = logfile.readLine()) != null && line.contains("output-file")) {
					splitLine = line.split("\t");
					String fileName = splitLine[5];
					if (fileNames) {
						fileName = fileName.substring(Math.max(0, fileName.lastIndexOf('/') + 1));
					}
					int fileSize = Integer.parseInt(splitLine[4]) / 1000;
					fileSize = (fileSize > 0) ? fileSize : 1;
					
					if (!fileNameToFile.containsKey(fileName)) {
						fileNameToFile.put(fileName, new org.cloudbus.cloudsim.File(fileName, fileSize));
						fileNameToConsumingTaskIds.put(fileName, new ArrayList<Integer>());
					}
					fileNameToProducingTaskId.put(fileName, cloudletId);
					outputSize += fileSize;
				}
				
//				inputSize = 0;
//				outputSize = 0;
				Task task = new Task(name, params, workflow, userId, cloudletId, timeInMs, (inputSize + outputSize), 0, 1, inputSize, outputSize, utilizationModel, utilizationModel, utilizationModel);
				taskIdToTask.put(cloudletId, task);
				workflow.addTask(task);
				cloudletId++;
			}
			
			logfile.close();
			
			for (String fileName : fileNameToFile.keySet()) {
				org.cloudbus.cloudsim.File file = fileNameToFile.get(fileName);
				
				ArrayList<Task> tasksRequiringThisFile = new ArrayList<Task>();
				for (Integer taskId : fileNameToConsumingTaskIds.get(fileName)) {
					tasksRequiringThisFile.add(taskIdToTask.get(taskId));
				}
				
				Task taskGeneratingThisFile = taskIdToTask.get(fileNameToProducingTaskId.get(fileName));
				
				if (taskGeneratingThisFile == null) {
					taskGeneratingThisFile = new Task("download", fileName, workflow, userId, cloudletId++, 0, file.getSize(), file.getSize(), 1, 0, file.getSize(), utilizationModel, utilizationModel, utilizationModel);
				}
				if (tasksRequiringThisFile.size() == 0 && fileName.matches(outputFileRegex)) {
					Task taskRequiringThisFile = new Task("upload", fileName, workflow, userId, cloudletId++, 0, file.getSize(), file.getSize(), 1, file.getSize(), 0, utilizationModel, utilizationModel, utilizationModel);
					tasksRequiringThisFile.add(taskRequiringThisFile);
				}
				workflow.addFile(file, taskGeneratingThisFile, tasksRequiringThisFile);
			}
			
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParameterException e) {
			e.printStackTrace();
		}
		
		return workflow;
	}

}
