package de.huberlin.wbi.dcs.workflow;

import org.cloudbus.cloudsim.File;
import org.cloudbus.cloudsim.ParameterException;

public class DataDependency {
	
	File file;
	
	public DataDependency(File file) throws ParameterException {
		setFile(file);
	}
	
	public String toString() {
		return getFile().getName();
	}
	
	public File getFile() {
		return file;
	}
	
	private void setFile(File file) {
		this.file = file;
	}
	
	@Override
	public int hashCode() {
		return getFile().getName().hashCode();
	}
	
}
