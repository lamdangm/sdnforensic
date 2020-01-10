package net.floodlightcontroller.ForenGuard;

import java.util.*;

import org.projectfloodlight.openflow.types.MacAddress;

enum DeviceType{
	SWITCH, HOST, ANY
}

public class PortProperty {
	
	 DeviceType device_type;  // this identify the type of device
	 Map<MacAddress,Boolean> hosts; //host list of this port, including mac address and disable flag
	
	public PortProperty ()
	{
		this.device_type = DeviceType.ANY;
		hosts = new HashMap<MacAddress, Boolean>();
	}
	
	protected DeviceType getDeviceType()
	{
		return this.device_type;
	}
	
	protected void setPortHost()
	{
		this.device_type = DeviceType.HOST;
	}
	
	protected void setPortSwitch()
	{
		this.device_type = DeviceType.SWITCH;
	}
	
	protected void setPortAny()
	{
		this.device_type = DeviceType.ANY;
	}
	
	protected void addHost(MacAddress mac)
	{
		this.hosts.put(mac, false);
	}
	
	
	protected void receivePortShutDown()
	{
		this.hosts.clear();
		setPortAny();
		
	}
	
	protected void enableHostShutDown(MacAddress mac){
		this.hosts.put(mac, true);
	}
	
	protected void disableHostShutDown(MacAddress mac){
		this.hosts.put(mac, false);
	}
	
	protected void receivePortDown(){
		for (MacAddress mac : this.hosts.keySet()){
			this.hosts.put(mac, true);
		}
	}
	
	protected void receiveTrafficFromPort(boolean isLLDP, MacAddress src)
	{
		//receivePortUp();
		if(isLLDP)
			setPortSwitch();
		else
		{
			setPortHost();
			if(!hosts.containsKey(src))
				hosts.put(src,false);
		}
			
	}

}