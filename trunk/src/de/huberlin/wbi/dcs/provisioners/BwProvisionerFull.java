package de.huberlin.wbi.dcs.provisioners;

import java.util.HashSet;
import java.util.Set;

import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.provisioners.BwProvisioner;

public class BwProvisionerFull extends BwProvisioner {
	
	private Set<String> vmNames;
	
	public BwProvisionerFull(long availableBw) {
		super(availableBw);
		setVmNames(new HashSet<String>());
	}
	
	@Override
	public boolean allocateBwForVm(Vm vm, long bw) {
		deallocateBwForVm(vm);
		if (getBw() >= bw) {
			getVmNames().add(vm.getUid());
			vm.setCurrentAllocatedBw(getBw());
			return true;
		}
		return false;
	}
	
	@Override
	public void deallocateBwForVm(Vm vm) {
		if (getVmNames().contains(vm.getUid())) {
			getVmNames().remove(vm.getUid());
			vm.setCurrentAllocatedBw(0);
		}
	}
	
	@Override
	public long getAllocatedBwForVm(Vm vm) {
		if (getVmNames().contains(vm.getUid())) {
			return getBw();
		}
		return 0;
	}
	
	@Override
	public boolean isSuitableForVm(Vm vm, long bw) {
		if (getBw() >= bw) {
			return true;
		}
		return false;
	}
	
	@Override
	public void deallocateBwForAllVms() {
		super.deallocateBwForAllVms();
		vmNames.clear();
	}
	
	public Set<String> getVmNames() {
		return vmNames;
	}
	
	public void setVmNames(Set<String> vmNames) {
		this.vmNames = vmNames;
	}

}
