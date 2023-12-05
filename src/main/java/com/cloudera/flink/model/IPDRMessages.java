package com.cloudera.flink.model;

import java.util.Arrays;
import java.util.Objects;

public class IPDRMessages {
    public String batchId;
    public IPv4v6 cmIp;
    public String cmMacAddr;
    public String cmtsHostName;
    public IPv4v6 cmtsIp;
    public int[] dsChSet;
    public Integer dsIdentifier;
    public Integer dsOctets;
    public Integer dsPackets;
    public String dsScn;
    public Integer dsTimeActive;
    public String fromTime;
    public Integer mdIfIndex;
    public String mdIfName;
    public Integer qosVersion;
    public String region;
    public String timeZone;
    public String toTime;
    public int[] usChSet;
    public Integer usIdentifier;
    public Integer usOctets;
    public Integer usPackets;
    public String usScn;
    public Integer usTimeActive;

    public Integer v;

    public IPDRMessages() {}
    public IPDRMessages(String batchId, IPv4v6 cmIp, String cmMacAddr, String cmtsHostName, IPv4v6 cmtsIp, int[] dsChSet, Integer dsIdentifier, Integer dsOctets, Integer dsPackets, String dsScn, Integer dsTimeActive, String fromTime, Integer mdIfIndex, String mdIfName, Integer qosVersion, String region, String timeZone, String toTime, int[] usChSet, Integer usIdentifier, Integer usOctets, Integer usPackets, String usScn, Integer usTimeActive, Integer v) {
        this.batchId = batchId;
        this.cmIp = cmIp;
        this.cmMacAddr = cmMacAddr;
        this.cmtsHostName = cmtsHostName;
        this.cmtsIp = cmtsIp;
        this.dsChSet = dsChSet;
        this.dsIdentifier =  dsIdentifier;
        this.dsOctets = dsOctets;
        this.dsPackets = dsPackets;
        this.dsScn = dsScn;
        this.dsTimeActive = dsTimeActive;
        this.fromTime = fromTime;
        this.mdIfIndex = mdIfIndex;
        this.mdIfName = mdIfName;
        this.qosVersion = qosVersion;
        this.region = region;
        this.timeZone = timeZone;
        this.toTime = toTime;
        this.usChSet = usChSet;
        this.usIdentifier = usIdentifier;
        this.usOctets = usOctets;
        this.usPackets = usPackets;
        this.usScn = usScn;
        this.usTimeActive = usTimeActive;
        this.v = v;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IPDRMessages that = (IPDRMessages) o;
        return batchId.equals(that.batchId) && cmIp.equals(that.cmIp) && cmMacAddr.equals(that.cmMacAddr) && cmtsHostName.equals(that.cmtsHostName) && cmtsIp.equals(that.cmtsIp) && Arrays.equals(dsChSet, that.dsChSet) && dsIdentifier.equals(that.dsIdentifier) && dsOctets.equals(that.dsOctets) && dsPackets.equals(that.dsPackets) && dsScn.equals(that.dsScn) && dsTimeActive.equals(that.dsTimeActive) && fromTime.equals(that.fromTime) && mdIfIndex.equals(that.mdIfIndex) && mdIfName.equals(that.mdIfName) && qosVersion.equals(that.qosVersion) && region.equals(that.region) && timeZone.equals(that.timeZone) && toTime.equals(that.toTime)  && Arrays.equals(usChSet, that.usChSet) && usIdentifier.equals(that.usIdentifier) && usOctets.equals(that.usOctets) && usPackets.equals(that.usPackets) && usScn.equals(that.usScn) && usTimeActive.equals(that.usTimeActive) && v.equals(that.v);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(batchId, cmIp, cmMacAddr, cmtsHostName, cmtsIp, dsIdentifier, dsOctets, dsPackets, dsScn, dsTimeActive, fromTime, mdIfIndex, mdIfName, qosVersion, region, timeZone, toTime, usIdentifier, usOctets, usPackets, usScn, usTimeActive, v);
        result = 31 * result + Arrays.hashCode(dsChSet);
        result = 31 * result + Arrays.hashCode(usChSet);
        return result;
    }

    @Override
    public String toString() {
        return "IPDRMessages{" +
                "batchId='" + batchId + '\'' +
                ", cmIp.V4=" + cmIp.getV4() +
                ", cmMacAddr='" + cmMacAddr + '\'' +
                ", cmtsHostName='" + cmtsHostName + '\'' +
                ", cmtsIp.V4=" + cmtsIp.getV4() +
                ", cmtsIp.V6=" + cmtsIp.getV6() +
                ", dsChSet=" + Arrays.toString(dsChSet) +
                ", dsIdentifier=" + dsIdentifier +
                ", dsOctets=" + dsOctets +
                ", dsPackets=" + dsPackets +
                ", dsScn='" + dsScn + '\'' +
                ", dsTimeActive=" + dsTimeActive +
                ", fromTime='" + fromTime + '\'' +
                ", mdIfIndex=" + mdIfIndex +
                ", mdIfName='" + mdIfName + '\'' +
                ", qosVersion=" + qosVersion +
                ", region= '" + region + '\'' +
                ", timeZone='" + timeZone + '\'' +
                ", toTime='" + toTime + '\'' +
                ", usChSet=" + Arrays.toString(usChSet) +
                ", usIdentifier=" + usIdentifier +
                ", usOctets=" + usOctets +
                ", usPackets=" + usPackets +
                ", usScn='" + usScn + '\'' +
                ", usTimeActive=" + usTimeActive +
                ", v=" + v +
                '}';
    }

    public String getBatchId() {
        return batchId;
    }

    public void setBatchId(String batchId) {
        this.batchId = batchId;
    }

    public String getCmMacAddr() {
        return cmMacAddr;
    }

    public void setCmMacAddr(String cmMacAddr) {
        this.cmMacAddr = cmMacAddr;
    }

    public String getCmtsHostName() {
        return cmtsHostName;
    }

    public void setCmtsHostName(String cmtsHostName) {
        this.cmtsHostName = cmtsHostName;
    }

    public Integer getDsOctets() {
        return dsOctets;
    }

    public void setDsOctets(Integer dsOctets) {
        this.dsOctets = dsOctets;
    }

    public Integer getDsPackets() {
        return dsPackets;
    }

    public void setDsPackets(Integer dsPackets) {
        this.dsPackets = dsPackets;
    }

    public String getDsScn() {
        return dsScn;
    }

    public void setDsScn(String dsScn) {
        this.dsScn = dsScn;
    }

    public Integer getV() {
        return v;
    }

    public void setV(Integer v) {
        this.v = v;
    }

    public IPv4v6 getCmIp() {
        return cmIp;
    }

    public void setCmIp(IPv4v6 cmIp) {
        this.cmIp = cmIp;
    }

    public IPv4v6 getCmtsIp() {
        return cmtsIp;
    }

    public void setCmtsIp(IPv4v6 cmtsIp) {
        this.cmtsIp = cmtsIp;
    }

    public int[] getDsChSet() {
        return dsChSet;
    }

    public void setDsChSet(int[] dsChSet) {
        this.dsChSet = dsChSet;
    }

    public Integer getDsIdentifier() {
        return dsIdentifier;
    }

    public void setDsIdentifier(Integer dsIdentifier) {
        this.dsIdentifier = dsIdentifier;
    }

    public Integer getDsTimeActive() {
        return dsTimeActive;
    }

    public void setDsTimeActive(Integer dsTimeActive) {
        this.dsTimeActive = dsTimeActive;
    }

    public String getFromTime() {
        return fromTime;
    }

    public void setFromTime(String fromTime) {
        this.fromTime = fromTime;
    }

    public Integer getMdIfIndex() {
        return mdIfIndex;
    }

    public void setMdIfIndex(Integer mdIfIndex) {
        this.mdIfIndex = mdIfIndex;
    }

    public String getMdIfName() {
        return mdIfName;
    }

    public void setMdIfName(String mdIfName) {
        this.mdIfName = mdIfName;
    }

    public Integer getQosVersion() {
        return qosVersion;
    }

    public void setQosVersion(Integer qosVersion) {
        this.qosVersion = qosVersion;
    }

    public String getTimeZone() {
        return timeZone;
    }

    public void setTimeZone(String timeZone) {
        this.timeZone = timeZone;
    }

    public String getToTime() {
        return toTime;
    }

    public void setToTime(String toTime) {
        this.toTime = toTime;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public int[] getUsChSet() {
        return usChSet;
    }

    public void setUsChSet(int[] usChSet) {
        this.usChSet = usChSet;
    }

    public Integer getUsIdentifier() {
        return usIdentifier;
    }

    public void setUsIdentifier(Integer usIdentifier) {
        this.usIdentifier = usIdentifier;
    }

    public Integer getUsOctets() {
        return usOctets;
    }

    public void setUsOctets(Integer usOctets) {
        this.usOctets = usOctets;
    }

    public Integer getUsPackets() {
        return usPackets;
    }

    public void setUsPackets(Integer usPackets) {
        this.usPackets = usPackets;
    }

    public String getUsScn() {
        return usScn;
    }

    public void setUsScn(String usScn) {
        this.usScn = usScn;
    }

    public Integer getUsTimeActive() {
        return usTimeActive;
    }

    public void setUsTimeActive(Integer usTimeActive) {
        this.usTimeActive = usTimeActive;
    }
}


