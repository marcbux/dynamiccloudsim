/*
 * Title:        CloudSim Toolkit
 * Description:  CloudSim (Cloud Simulation) Toolkit for Modeling and Simulation of Clouds
 * Licence:      GPL - http://www.gnu.org/copyleft/gpl.html
 *
 * Copyright (c) 2009-2012, The University of Melbourne, Australia
 */

package de.huberlin.wbi.dcs.core.predicates;

import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.core.predicates.Predicate;

public class PredicateTime extends Predicate {

	private final double time;
	private final double epsilon;
	
	public PredicateTime(double time, double epsilon) {
		this.time = time;
		this.epsilon = epsilon;
	}

	@Override
	public boolean match(SimEvent ev) {
		if (Math.abs(this.time - ev.eventTime()) < epsilon) {
			return true;
		}
		return false;
	}

}
