package de.huberlin.wbi.dcs.workflow.io;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.cloudbus.cloudsim.ParameterException;

import de.huberlin.wbi.dcs.workflow.Task;
import de.huberlin.wbi.dcs.workflow.Workflow;

public class MontageTraceFileReader extends LogFileReader {
	
	@Override
	protected void fillDataStructures(int userId, String filePath, boolean fileNames, boolean kernelTime, Workflow workflow) {
		try {
			BufferedReader logfile = new BufferedReader(new FileReader(filePath));
			
			String line = logfile.readLine();
			String[] splitLine;
			
			while (line != null) {
				splitLine = line.split(" : ");
				String name = splitLine[1];
				String params = "";
				if (splitLine.length > 3) {
					params = splitLine[3];
				}
				
				int timeInMs = 0;
				int inputSize = 0;
				int outputSize = 0;
				
				while ((line = logfile.readLine()).contains("input")) {
					splitLine = line.split(" : ");
					String fileName = splitLine[3];
					if (fileNames) {
						fileName = fileName.substring(Math.max(0, fileName.lastIndexOf('/') + 1));
					}
					int fileSize = 1;
					if (splitLine.length > 4) {
						fileSize = Integer.parseInt(splitLine[4]) / 1024;
					}
					fileSize = (fileSize > 0) ? fileSize : 1;
					
					if (!fileNameToFile.containsKey(fileName)) {
						fileNameToFile.put(fileName, new org.cloudbus.cloudsim.File(fileName, fileSize));
						fileNameToConsumingTaskIds.put(fileName, new ArrayList<Long>());
					}
					fileNameToConsumingTaskIds.get(fileName).add((long)cloudletId);
					inputSize += fileSize;
				}
				
				for (int i = 0; i < 2; i++) {
					String[] userTime = logfile.readLine().split("\t")[1].split("m|\\.|s");
					timeInMs += Integer.parseInt(userTime[0]) * 60 * 1000 + Integer.parseInt(userTime[1]) * 1000 + Integer.parseInt(userTime[2]);
					if (!kernelTime) {
						logfile.readLine();
						break;
					}
				}
				
				while ((line = logfile.readLine()) != null && line.contains("output")) {
					splitLine = line.split(" : ");
					String fileName = splitLine[3];
					if (fileNames) {
						fileName = fileName.substring(Math.max(0, fileName.lastIndexOf('/') + 1));
					}
					int fileSize = 1;
					if (splitLine.length > 4) {
						fileSize = Integer.parseInt(splitLine[4]) / 1024;
					}
					fileSize = (fileSize > 0) ? fileSize : 1;
					
					if (!fileNameToFile.containsKey(fileName)) {
						fileNameToFile.put(fileName, new org.cloudbus.cloudsim.File(fileName, fileSize));
						fileNameToConsumingTaskIds.put(fileName, new ArrayList<Long>());
					}
					fileNameToProducingTaskId.put(fileName, (long)cloudletId);
					outputSize += fileSize;
				}
				
				Task task = new Task(name, params, workflow, userId, cloudletId, timeInMs, (inputSize + outputSize), 0, 1, inputSize, outputSize, utilizationModel, utilizationModel, utilizationModel);
				taskIdToTask.put((long)cloudletId, task);
				cloudletId++;
			}
			
			logfile.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParameterException e) {
			e.printStackTrace();
		}
	}

}
