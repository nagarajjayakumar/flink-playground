//This is the output structure for the
package com.cloudera.flink.model;

import java.util.Objects;

public class OutputUsageMessages {
	public String hourUsage;
	public String cmtshost;
	public String scn;
	public String mac;
	public long usage;

	public String getHourUsage() {
		return hourUsage;
	}

	public void setHourUsage(String hourUsage) {
		this.hourUsage = hourUsage;
	}

	public String getCmtshost() {
		return cmtshost;
	}

	public void setCmtshost(String cmtshost) {
		this.cmtshost = cmtshost;
	}

	public String getScn() {
		return scn;
	}

	public void setScn(String scn) {
		this.scn = scn;
	}

	public String getMac() {
		return mac;
	}

	public void setMac(String mac) {
		this.mac = mac;
	}

	public long getUsage() {
		return usage;
	}

	public void setUsage(long usage) {
		this.usage = usage;
	}

	public OutputUsageMessages() { }
	public OutputUsageMessages(String time, String cmtshost, String scn, String mac, long usage) {
		this.hourUsage = time;
		this.cmtshost = cmtshost;
		this.scn = scn;
		this.mac = mac;
		this.usage = usage;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		OutputUsageMessages that = (OutputUsageMessages) o;
		return hourUsage == that.hourUsage && usage == that.usage && cmtshost.equals(that.cmtshost) && scn.equals(that.scn) && mac.equals(that.mac);
	}

	@Override
	public int hashCode() {
		return Objects.hash(hourUsage, cmtshost, scn, mac, usage);
	}

	@Override
	public String toString() {
		return "HourlyUsage{" +
				"hour='" + hourUsage + '\'' +
				", cmtshost='" + cmtshost + '\'' +
				", scn='" + scn + '\'' +
				", mac='" + mac + '\'' +
				", usage=" + usage +
				'}';
	}
}