package de.huberlin.wbi.dcs.workflow.io;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.cloudbus.cloudsim.ParameterException;
import org.cloudbus.cloudsim.UtilizationModel;
import org.cloudbus.cloudsim.UtilizationModelFull;

import de.huberlin.wbi.dcs.workflow.Task;
import de.huberlin.wbi.dcs.workflow.Workflow;

public class AlignmentTraceFileReader extends LogFileReader {

	@Override
	protected void fillDataStructures(int userId, String filePath,
			boolean fileNames, boolean kernelTime, Workflow workflow) {
		try {
			BufferedReader logfile = new BufferedReader(
					new FileReader(filePath));

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
						fileName = fileName.substring(Math.max(0,
								fileName.lastIndexOf('/') + 1));
					}
					int fileSize = Integer.parseInt(splitLine[4]) / 1024;
					fileSize = (fileSize > 0) ? fileSize : 1;

					if (!fileNameToFile.containsKey(fileName)) {
						fileNameToFile.put(fileName,
								new org.cloudbus.cloudsim.File(fileName,
										fileSize));
						fileNameToConsumingTaskIds.put(fileName,
								new ArrayList<Long>());
					}
					fileNameToConsumingTaskIds.get(fileName).add(
							(long) cloudletId);
					inputSize += fileSize;
				} while ((line = logfile.readLine()).contains("input-file"));

				for (int i = 0; i < 2; i++) {
					splitLine = logfile.readLine().split("\t");
					if (kernelTime || i < 1) {
						timeInMs += (int) (Float.parseFloat(splitLine[4]) * 1000);
					}
				}

				while ((line = logfile.readLine()) != null
						&& line.contains("output-file")) {
					splitLine = line.split("\t");
					String fileName = splitLine[5];
					if (fileNames) {
						fileName = fileName.substring(Math.max(0,
								fileName.lastIndexOf('/') + 1));
					}
					int fileSize = Integer.parseInt(splitLine[4]) / 1024;
					fileSize = (fileSize > 0) ? fileSize : 1;

					if (!fileNameToFile.containsKey(fileName)) {
						fileNameToFile.put(fileName,
								new org.cloudbus.cloudsim.File(fileName,
										fileSize));
						fileNameToConsumingTaskIds.put(fileName,
								new ArrayList<Long>());
					}
					fileNameToProducingTaskId.put(fileName, (long) cloudletId);
					outputSize += fileSize;
				}

				Task task = new Task(name, params, workflow, userId,
						cloudletId, timeInMs, (inputSize + outputSize), 0, 1,
						inputSize, outputSize, utilizationModel,
						utilizationModel, utilizationModel);
				taskIdToTask.put((long) cloudletId, task);
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
