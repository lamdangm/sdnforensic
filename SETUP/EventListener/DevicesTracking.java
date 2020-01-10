package net.floodlightcontroller.ForenGuard;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Timestamp;
import java.util.*;
import java.util.Map.Entry;

import org.bson.Document;
import org.projectfloodlight.openflow.protocol.OFMessage;
import org.projectfloodlight.openflow.protocol.OFPacketIn;
import org.projectfloodlight.openflow.protocol.OFPortDesc;
import org.projectfloodlight.openflow.protocol.OFType;
import org.projectfloodlight.openflow.protocol.OFVersion;
import org.projectfloodlight.openflow.protocol.match.MatchField;
import org.projectfloodlight.openflow.types.DatapathId;
import org.projectfloodlight.openflow.types.MacAddress;
import org.projectfloodlight.openflow.types.OFPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.mongodb.client.MongoCollection;

import net.floodlightcontroller.core.FloodlightContext;
import net.floodlightcontroller.core.IFloodlightProviderService;
import net.floodlightcontroller.core.IOFMessageListener;
import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.IOFSwitch.SwitchStatus;
import net.floodlightcontroller.core.IOFSwitchListener;
import net.floodlightcontroller.core.PortChangeType;
import net.floodlightcontroller.core.internal.IOFSwitchManager;
import net.floodlightcontroller.core.internal.IOFSwitchService;
import net.floodlightcontroller.core.module.FloodlightModuleContext;
import net.floodlightcontroller.core.module.FloodlightModuleException;
import net.floodlightcontroller.core.module.IFloodlightModule;
import net.floodlightcontroller.core.module.IFloodlightService;
import net.floodlightcontroller.devicemanager.IDevice;
import net.floodlightcontroller.devicemanager.IDeviceListener;
import net.floodlightcontroller.devicemanager.IDeviceService;
import net.floodlightcontroller.devicemanager.SwitchPort;
import net.floodlightcontroller.linkdiscovery.ILinkDiscoveryListener;
import net.floodlightcontroller.linkdiscovery.ILinkDiscoveryService;
import net.floodlightcontroller.packet.ARP;
import net.floodlightcontroller.packet.Data;
import net.floodlightcontroller.packet.Ethernet;
import net.floodlightcontroller.packet.ICMP;
import net.floodlightcontroller.packet.IPacket;
import net.floodlightcontroller.packet.IPv4;
import net.floodlightcontroller.packet.LLDP;

import net.floodlightcontroller.linkdiscovery.ILinkDiscoveryListener;
import net.floodlightcontroller.linkdiscovery.ILinkDiscoveryService;

public class DevicesTracking implements IDeviceListener,IFloodlightModule {

	protected IFloodlightProviderService floodlightProvider;
	protected IDeviceService deviceService;
	protected IOFSwitchService SwitchSvc;
	private static final Logger logger = LoggerFactory.getLogger(DevicesTracking.class.getName());
    protected FloodlightContext cntx;    
    protected Timestamp ts ;
    protected PortManager portMan;
    protected MongoDb mgdb;
    class Port {
    	DatapathId sw;			
    	OFPort port_number;	
    	
    	public Port(DatapathId dpid, OFPort port) {
    		this.sw = dpid;
    		this.port_number = port;
    	}

    	@Override
    	public boolean equals(Object obj) {
    		if (obj == null) {
    			return false;
    		}
    		if (getClass() != obj.getClass()) {
    			return false;
    		}
    		final Port other = (Port) obj;
    		if (this.sw != other.sw) {
    			return false;
    		}
    		if (this.port_number != other.port_number) {
    			return false;
    		}
    		return true;
    	}

    };
	
    @Override
	public String getName() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isCallbackOrderingPrereq(String type, String name) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isCallbackOrderingPostreq(String type, String name) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Collection<Class<? extends IFloodlightService>> getModuleServices() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<Class<? extends IFloodlightService>, IFloodlightService> getServiceImpls() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Collection<Class<? extends IFloodlightService>> getModuleDependencies() {
		// TODO Auto-generated method stub
		Collection<Class<? extends IFloodlightService>> l = new ArrayList<Class<? extends IFloodlightService>>();
		l.add(IFloodlightProviderService.class);
		l.add(ILinkDiscoveryService.class);
		l.add(IDeviceService.class);
		return l;
	}

	@Override
	public void init(FloodlightModuleContext context) throws FloodlightModuleException {
		// TODO Auto-generated method stub
		floodlightProvider = context
				.getServiceImpl(IFloodlightProviderService.class);
		deviceService = context.getServiceImpl(IDeviceService.class);
		SwitchSvc = context.getServiceImpl(IOFSwitchService.class);
		portMan = new PortManager();
		
	}

	@Override
	public void startUp(FloodlightModuleContext context) throws FloodlightModuleException {
		// TODO Auto-generated method stub
		deviceService.addListener(this);
		floodlightProvider.addOFMessageListener(OFType.PACKET_IN,
				this.portMan);
		SwitchSvc.addOFSwitchListener(this.portMan);
	}

	@Override
	public void deviceAdded(IDevice device) {
		// TODO Auto-generated method stub
		logger.info("Device:{} is added on:", device.getMACAddressString());
		for (SwitchPort sp : device.getAttachmentPoints()) {
			logger.info("sw: {}, port: {}", sp.getNodeId(), sp.getPortId());
		}
	}

	@Override
	public void deviceRemoved(IDevice device) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void deviceMoved(IDevice device) {
		// TODO Auto-generated method stub		
		MacAddress src_mac = device.getMACAddress();
		SwitchPort[] previousLocation = device.getOldAP();
		if (previousLocation.length == 1) { // only one location in AP history
			DatapathId previousDpid= previousLocation[0].getNodeId();
			OFPort previousPortID = previousLocation[0].getPortId();
			Port previousPort = new Port(previousDpid, previousPortID);
			logger.info("Previous Sw: {} - Port {}",previousDpid,previousPort);
			Map<MacAddress, Boolean> hosts = new HashMap<MacAddress, Boolean>();
			for (Entry<Port,PortProperty> e : this.portMan.port_list.entrySet()) {
				Port ePort = e.getKey();
				if(ePort.equals(previousPort)) {
					hosts = this.portMan.port_list.get(ePort).hosts;
					break;
				}
				
			}
			
			if(hosts == null) {
				logger.warn("Error While Get Host List On Switch");
			}
			if (!hosts.containsKey(src_mac)){
				logger.error("can not read previous host location about {}",src_mac);
				return;
			}
			if (hosts.get(src_mac) == false) {	 
				//Violation: no port shutdown signal is received during host move
				//logger.warn("Violation: Host Move from switch {} port {} without Port ShutDown"
						//, previousDpid, previousPortID);
				mgdb = new MongoDb("192.168.8.128",27017,"event","fdb","123456");
				MongoCollection<Document> coll = mgdb.GetCollection("fdb","HostData");				
				Document doc = Document.parse(DeviceJson(src_mac,previousPort,"LOCATION-HOST"));
				coll.insertOne(doc);
				mgdb.Disconect();
				
			}
		}
	}

	@Override
	public void deviceIPV4AddrChanged(IDevice device) {
		// TODO Auto-generated method stub

	}

	@Override
	public void deviceIPV6AddrChanged(IDevice device) {
		// TODO Auto-generated method stub

	}

	@Override
	public void deviceVlanChanged(IDevice device) {
		// TODO Auto-generated method stub
		
	}
	@SuppressWarnings("null")
	public String DeviceJson(MacAddress Mac,Port sw_port,String Event) {
		JsonFactory factory = new JsonFactory();
		ByteArrayOutputStream stm = new ByteArrayOutputStream();
		ts = new Timestamp(System.currentTimeMillis());
		switch(Event) {
			case "LLDP-HOST":
				try {
					JsonGenerator jGen = factory.createGenerator(stm);
					jGen.writeStartObject();
					jGen.writeStringField("Timestamp",ts.toString());
					jGen.writeStringField("Type", "OFSwitch_Diagnostic");
					jGen.writeStringField("Event", "LLDP-HOST");
					jGen.writeStringField("Description", "LLDP Sending From Host - Possible Link Fabrication Attack");
					jGen.writeFieldName("Device");
						jGen.writeStartObject();
							jGen.writeFieldName("Attachment-Point");
							jGen.writeStartObject();
								jGen.writeStringField("SwitchId", sw_port.sw.toString());
								jGen.writeNumberField("PortNumber", sw_port.port_number.getPortNumber());
								jGen.writeStringField("PortMacAddress",Mac.toString());
								jGen.writeEndObject();
						jGen.writeEndObject();
					jGen.writeEndObject();
					jGen.close();
					
				}
				catch(IOException e){
					e.printStackTrace();
					break;
				}
				break;
			case "LOCATION-HOST":
				try {
					JsonGenerator jGen = factory.createGenerator(stm);
					jGen.writeStartObject();
					jGen.writeStringField("Timestamp",ts.toString());
					jGen.writeStringField("Type", "OFHost_Diagnostic");
					jGen.writeStringField("Event", "LOCATION-HOST");
					jGen.writeStringField("Description", "Host Move Without Port Shutdown - Possible Host Location Hijacking");
					jGen.writeFieldName("Device");
						jGen.writeStartObject();
							jGen.writeStringField("MacAddress",Mac.toString());
							jGen.writeFieldName("Attachment-Point");
							jGen.writeStartObject();
								jGen.writeStringField("Last-SwitchID", sw_port.sw.toString());
								jGen.writeNumberField("Last-Port-Number", sw_port.port_number.getPortNumber());
							jGen.writeEndObject();
						jGen.writeEndObject();
					jGen.writeEndObject();
					jGen.close();
				}catch(IOException e) {
					e.printStackTrace();
					break;
				}
				break;
			default:
				break;
		}
		return stm.toString();
	
	}
	class PortManager implements IOFMessageListener, IOFSwitchListener {

		protected Map<Port, PortProperty> port_list; 
		
		protected Map<MacAddress, Port> mac_port;		
		public PortManager() {
			port_list = new HashMap<Port, PortProperty>();
			mac_port = new HashMap<MacAddress, Port>();
		}
			@Override
			public String getName() {
				// TODO Auto-generated method stub
				return "PortManager";
			}
			@Override
			public boolean isCallbackOrderingPrereq(OFType type, String name) {
				// TODO Auto-generated method stub
				return false;
			}
			@Override
			public boolean isCallbackOrderingPostreq(OFType type, String name) {
				// TODO Auto-generated method stub
				return (type.equals(OFType.PACKET_IN) && (name.equals("topology")
						|| name.equals("linkdiscovery") || name
							.equals("devicemanager")));
			}
			@Override
			public Command receive(IOFSwitch sw, OFMessage msg, FloodlightContext cntx) {
				// TODO Auto-generated method stub
				switch (msg.getType()) {
				
				case PACKET_IN:
					return this.processPacketInMessage(sw, (OFPacketIn) msg, cntx);
				default:
					break;
				}

				return Command.CONTINUE;
			}
			Command processPacketInMessage(IOFSwitch sw, OFPacketIn pi,
					FloodlightContext cntx) {
				Ethernet eth = IFloodlightProviderService.bcStore.get(cntx,
						IFloodlightProviderService.CONTEXT_PI_PAYLOAD);
				DatapathId dpid = sw.getId();
				OFPort inPort = (pi.getVersion().compareTo(OFVersion.OF_12) < 0 ? pi.getInPort() : pi.getMatch().get(MatchField.IN_PORT));
				Port sw_port = new Port(dpid, inPort);
				MacAddress src_mac = eth.getSourceMACAddress();
				
				//this.port_list.get() Replaces
				PortProperty pp = new PortProperty();
				for (Entry<Port, PortProperty> e : this.port_list.entrySet()) {
					Port EntryPort = e.getKey();
					if(EntryPort.equals(sw_port)) {
						 pp = this.port_list.get(EntryPort);
						if(pp == null) {	
							return Command.CONTINUE;
						}											
					}
				}
				DeviceType dt = pp.getDeviceType();
				logger.info("Device Type: {} ------ Mac: {} ",dt,src_mac);
				if (eth.isBroadcast())
					return Command.CONTINUE;					

				if (eth.getPayload() instanceof LLDP) {
					if (dt == DeviceType.ANY) {
						pp.setPortSwitch();
					} else if (dt == DeviceType.HOST) {
						logger.warn("Violation: Receive LLDP packets from HOST port: SW {} port {}"
								, dpid, inPort);
						mgdb = new MongoDb("192.168.8.128",27017,"event","fdb","123456");
						MongoCollection<Document> coll = mgdb.GetCollection("fdb","DiagnosticData");				
						Document doc = Document.parse(DeviceJson(src_mac,sw_port,"LLDP-HOST"));
						coll.insertOne(doc);
						mgdb.Disconect();
						//return Command.STOP;
					}
				}
				else {
					if (this.mac_port.containsKey(src_mac)) {
						Port host_location = this.mac_port.get(src_mac);
						if(host_location.equals(sw_port)) {
							// this port is first hop port for found host
							if (dt == DeviceType.SWITCH) {
								// Violation:  receive first hop traffic from SWITH port
								logger.warn("Violation: Receive first hop host packets from SWITCH port: SW {} port {}"
										, dpid, inPort);
								//return Command.STOP;
							} else if (dt == DeviceType.ANY) {
								pp.setPortHost();
								pp.disableHostShutDown(src_mac);
								this.port_list.put(sw_port, pp);
							}	
						}
					} 
					else { // new host
						this.mac_port.put(src_mac, sw_port);
						pp.addHost(src_mac);	
						pp.setPortAny();
						this.port_list.put(sw_port, pp);
					}
					
				}
				return Command.CONTINUE;
			}
			@Override
			public void switchAdded(DatapathId switchId) {
				logger.info("Switch Adding......");
				this.handleSwitchAdd(switchId);
			}
			@Override
			public void switchRemoved(DatapathId switchId) {
				// TODO Auto-generated method stub
				
			}
			@Override
			public void switchActivated(DatapathId switchId) {
				// TODO Auto-generated method stub
				
			}
			@Override
			public void switchPortChanged(DatapathId switchId, OFPortDesc port, PortChangeType type) {
				// TODO Auto-generated method stub
				switch(type) {
				case DOWN:
					this.handlePortDown(switchId, port.getPortNo());
					break;
				default:
					break;
				}
			}
			@Override
			public void switchChanged(DatapathId switchId) {
				// TODO Auto-generated method stub
				
			}
			@Override
			public void switchDeactivated(DatapathId switchId) {
				// TODO Auto-generated method stub
				
			}
			void handleSwitchAdd(DatapathId dpid) {
				IOFSwitch sw = SwitchSvc.getSwitch(dpid);
				for (OFPortDesc port : sw.getPorts()) {
					Port switch_port = new Port(sw.getId(),port.getPortNo());
					if (!this.port_list.containsKey(switch_port)) {
						this.port_list.put(switch_port, new PortProperty());
					}
				}
			}
			void handlePortDown(DatapathId dpid, OFPort  port) {
				Port switch_port = new Port(dpid, port);			
				PortProperty pp = new PortProperty();
				for(Entry<Port,PortProperty> e : this.port_list.entrySet()) {
					Port ePort = e.getKey();
					if(ePort.equals(switch_port)) {
						pp = this.port_list.get(ePort);
						pp.receivePortDown();
						break;
					}
				}
				this.port_list.put(switch_port, pp);
			}
	}
}
