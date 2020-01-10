package net.floodlightcontroller.statistics;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.Thread.State;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.io.IOException;

import org.projectfloodlight.openflow.protocol.OFFlowStatsEntry;
import org.projectfloodlight.openflow.protocol.OFFlowStatsReply;
import org.projectfloodlight.openflow.protocol.OFMessage;
import org.projectfloodlight.openflow.protocol.OFMatchType;
import org.projectfloodlight.openflow.protocol.OFPacketIn;
import org.projectfloodlight.openflow.protocol.OFPortDesc;
import org.projectfloodlight.openflow.protocol.OFPortDescProp;
import org.projectfloodlight.openflow.protocol.OFPortStatsEntry;
import org.projectfloodlight.openflow.protocol.OFPortStatsReply;
import org.projectfloodlight.openflow.protocol.OFStatsReply;
import org.projectfloodlight.openflow.protocol.OFStatsRequest;
import org.projectfloodlight.openflow.protocol.OFStatsType;
import org.projectfloodlight.openflow.protocol.OFType;
import org.projectfloodlight.openflow.protocol.OFVersion;
import org.projectfloodlight.openflow.protocol.match.Match;
import org.projectfloodlight.openflow.protocol.match.MatchField;
import org.projectfloodlight.openflow.protocol.ver13.OFMeterSerializerVer13;
import org.projectfloodlight.openflow.types.DatapathId;
import org.projectfloodlight.openflow.types.EthType;
import org.projectfloodlight.openflow.types.IPv4Address;
import org.projectfloodlight.openflow.types.IPv6Address;
import org.projectfloodlight.openflow.types.IpProtocol;
import org.projectfloodlight.openflow.types.MacAddress;
import org.projectfloodlight.openflow.types.OFGroup;
import org.projectfloodlight.openflow.types.OFPort;
import org.projectfloodlight.openflow.types.TableId;
import org.projectfloodlight.openflow.types.TransportPort;
import org.projectfloodlight.openflow.types.VlanVid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.primitives.UnsignedLong;
import com.google.common.util.concurrent.ListenableFuture;
import com.kenai.jffi.Array;

import javafx.util.Pair;
import net.floodlightcontroller.core.FloodlightContext;
import net.floodlightcontroller.core.IFloodlightProviderService;
import net.floodlightcontroller.core.IOFMessageListener;
import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.IOFSwitchListener;
import net.floodlightcontroller.core.PortChangeType;
import net.floodlightcontroller.core.IListener.Command;
import net.floodlightcontroller.core.internal.IOFSwitchService;
import net.floodlightcontroller.core.module.FloodlightModuleContext;
import net.floodlightcontroller.core.module.FloodlightModuleException;
import net.floodlightcontroller.core.module.IFloodlightModule;
import net.floodlightcontroller.core.module.IFloodlightService;
import net.floodlightcontroller.core.types.SwitchMessagePair;
import net.floodlightcontroller.core.types.NodePortTuple;

import net.floodlightcontroller.linkdiscovery.ILinkDiscovery;
import net.floodlightcontroller.linkdiscovery.ILinkDiscovery.LDUpdate;
import net.floodlightcontroller.packet.BSN;
import net.floodlightcontroller.packet.Ethernet;
import net.floodlightcontroller.packet.LLDP;
import net.floodlightcontroller.statistics.IStatisticsService;


import net.floodlightcontroller.topology.ITopologyListener;
import net.floodlightcontroller.topology.ITopologyService;
import net.floodlightcontroller.topology.TopologyManager;
import net.floodlightcontroller.statistics.StatisticsCollector;
import net.floodlightcontroller.util.ConcurrentCircularBuffer;
import net.floodlightcontroller.threadpool.IThreadPoolService;
import net.floodlightcontroller.core.IFloodlightProviderService;


import com.mongodb.*;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Filters;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Projections.*;
import com.mongodb.client.model.Sorts;
import com.mongodb.util.JSON;

import java.util.Arrays;
import org.bson.Document;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.concurrent.Executors;



public class neventcapture implements IFloodlightModule, IOFSwitchListener, ITopologyListener {

	protected IFloodlightProviderService floodlightProvider;
	private static final Logger logger = LoggerFactory.getLogger(neventcapture.class.getName());
	protected ConcurrentCircularBuffer<SwitchMessagePair> buffer;
	protected IOFSwitchService SwitchSvc;
	protected ITopologyService TopoSvc;
	private static IThreadPoolService ThreadPoolSvc;
	protected StatisticsCollector StatsCol;
    protected Timestamp ts ;
    
    
    private class GetStatisticsThread extends Thread {
		private List<OFStatsReply> statsReply;
		private DatapathId switchId;
		private OFStatsType statType;

		public GetStatisticsThread(DatapathId switchId, OFStatsType statType) {
			this.switchId = switchId;
			this.statType = statType;
			this.statsReply = null;
		}

		public List<OFStatsReply> getStatisticsReply() {
			return statsReply;
		}

		public DatapathId getSwitchId() {
			return switchId;
		}

		@Override
		public void run() {
			statsReply = getSwitchStatistics(switchId, statType);
		}
	}
    private class MongoTask {
    	@SuppressWarnings("deprecation")
    	
    	private String User;
    	private String Database;
    	private String Pwd;
    	protected MongoClient mgc;
    	
    	protected Block<Document> printBlock = new Block<Document>() {
 	       @Override
 	       public void apply(final Document document) {
 	           logger.info("{}",document.toJson().toString());
 	       }
    	};
    	
    	MongoTask(String Username, String DatabaseName, String PassWord){
    		User = Username;
    		Database = DatabaseName;
    		Pwd = PassWord;
    		mgc = Connect("192.168.8.128",27017);
    	}
    	
    	private MongoClient Connect(String HostIp,int Port) {
    		MongoClient mongoClient = null;
    		try {
    			MongoCredential mongoCred = MongoCredential.createCredential(User, Database, Pwd.toCharArray());
        		mongoClient = new MongoClient(new ServerAddress(HostIp, Port), Arrays.asList(mongoCred));
    		}catch (MongoException e) {
    			logger.error("Connect DB Fail",e);
    		}
    		return mongoClient;
    	}
    	
    	public void QueryCollection(String DatabaseName, String CollectionName){  
    		MongoDatabase mongodb = mgc.getDatabase(DatabaseName);
    		MongoCollection<Document> collection = mongodb.getCollection(CollectionName);
    		collection.find().forEach(printBlock);
    	}
    	public MongoCollection<Document> GetCollection(String DatabaseName, String CollectionName){  
    		MongoDatabase mongodb = mgc.getDatabase(DatabaseName);
    		MongoCollection<Document> collection = mongodb.getCollection(CollectionName);
    		return collection;
    	}
    	public void Disconect() {
    		mgc.close();
    	}
    	
    }
    
	
	@Override
	public void switchAdded(DatapathId switchId) {
		// TODO Auto-generated method stub
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
			MongoTask mgt = new MongoTask("event","fdb","123456");
			MongoCollection<Document> coll = mgt.GetCollection("fdb","SwitchData");				
			Document doc = Document.parse(SwitchDataJson(switchId,port,type));
			coll.insertOne(doc);
			mgt.Disconect();
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
	    l.add(IOFSwitchService.class);
	    l.add(ITopologyService.class);
	    return l;
	}

	@Override
	public void init(FloodlightModuleContext context) throws FloodlightModuleException {
		// TODO Auto-generated method stub
		 floodlightProvider = context.getServiceImpl(IFloodlightProviderService.class);
		 SwitchSvc = context.getServiceImpl(IOFSwitchService.class);
	     TopoSvc = context.getServiceImpl(ITopologyService.class);
	     ThreadPoolSvc = context.getServiceImpl(IThreadPoolService.class);
		 buffer = new ConcurrentCircularBuffer<SwitchMessagePair>(SwitchMessagePair.class, 100);
	}

	@Override
	public void startUp(FloodlightModuleContext context) throws FloodlightModuleException {
		// TODO Auto-generated method stub
        TopoSvc.addListener(this);
        SwitchSvc.addOFSwitchListener(this);       
	}
	
	@Override
	public void topologyChanged(List<LDUpdate> linkUpdates) {
		// TODO Auto-generated method stub
		logger.warn("Debugging Something Happen In Topology");
//		for(LDUpdate ldi : linkUpdates)	{
//			if(ldi.getOperation().equals(ILinkDiscovery.UpdateOperation.PORT_DOWN)){			
//				MongoTask mgt = new MongoTask("event","fdb","123456");
//				mgt.QueryCollection("fdb","SwitchData");
//				MongoCollection<Document> coll = mgt.GetCollection("fdb","SwitchData");				
//				Document doc = Document.parse(SwitchDataJson(ldi.getSrc(),ldi));
//				coll.insertOne(doc);
//				mgt.Disconect();
//			}
		}
	
	@SuppressWarnings("null")
	public String SwitchDataJson(DatapathId dpid, LDUpdate ldi)  {
		ByteArrayOutputStream stm = new ByteArrayOutputStream();
		JsonFactory factory = new JsonFactory();
		List<OFStatsReply> FlowReply = getSwitchStatistics(dpid,OFStatsType.FLOW);
		List<OFStatsReply> PortReply = getSwitchStatistics(dpid,OFStatsType.PORT);
		try {
			ts = new Timestamp(System.currentTimeMillis());
			JsonGenerator jGen = factory.createGenerator(stm) ;
			jGen.writeStartObject();
			jGen.writeStringField("Event", ldi.getOperation().toString());
			jGen.writeStringField("Timestamp",ts.toString());
			jGen.writeStringField("Type","OFSwitch_Stats");
			jGen.writeStringField("Description",ldi.getOperation().toString() +" - On Switch: "+ dpid.toString() +" - Port Number: " + ldi.getSrcPort().toString());
			jGen.writeFieldName("Switch");
			jGen.writeStartObject();
				jGen.writeStringField("DatapathId", dpid.toString());
				//Flow
				jGen.writeFieldName("Flow");
				jGen.writeStartArray();
				for(OFStatsReply fr : FlowReply) {
					OFFlowStatsReply fsr = (OFFlowStatsReply) fr;
					for(OFFlowStatsEntry fse : fsr.getEntries())
					{
						jGen.writeStartObject();
						jGen.writeStringField("Version",fse.getVersion().toString());
						jGen.writeNumberField("Cookie", fse.getCookie().getValue());
						jGen.writeStringField("Table_Id", fse.getTableId().toString());
						jGen.writeNumberField("Packet_count", fse.getPacketCount().getValue());
						jGen.writeNumberField("Byte_count", fse.getByteCount().getValue());
						jGen.writeNumberField("Duration", fse.getDurationSec());
						jGen.writeNumberField("Priority", fse.getPriority());
						jGen.writeEndObject();
					}
				}
				jGen.writeEndArray();
				//Port
				jGen.writeFieldName("Port");
					jGen.writeStartArray();
						for(OFStatsReply pr : PortReply) {
							OFPortStatsReply psr = (OFPortStatsReply) pr;
							for(OFPortStatsEntry pse : psr.getEntries())
							{
								jGen.writeStartObject();
								jGen.writeStringField("Port_Number", pse.getPortNo().toString());
								jGen.writeNumberField("Receive_Packets", pse.getRxPackets().getValue());
								jGen.writeNumberField("Transmit_Packets",pse.getTxPackets().getValue());
								jGen.writeNumberField("Receive_Dropped", pse.getRxDropped().getValue());
								jGen.writeNumberField("Transmit_Dropped", pse.getTxDropped().getValue());
								jGen.writeNumberField("Duration", pse.getDurationSec());
								jGen.writeEndObject();
							}
						}
					jGen.writeEndArray();
					jGen.writeEndObject();	
			jGen.writeEndObject();			
			jGen.close();
		} catch (IOException e ) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return stm.toString();
	}
	@SuppressWarnings("null")
	public String SwitchDataJson(DatapathId dpid,OFPortDesc Port,PortChangeType type)  {
		ByteArrayOutputStream stm = new ByteArrayOutputStream();
		JsonFactory factory = new JsonFactory();
		List<OFStatsReply> FlowReply = getSwitchStatistics(dpid,OFStatsType.FLOW);
		List<OFStatsReply> PortReply = getSwitchStatistics(dpid,OFStatsType.PORT);
		try {
			ts = new Timestamp(System.currentTimeMillis());
			JsonGenerator jGen = factory.createGenerator(stm) ;
			jGen.writeStartObject();
			jGen.writeStringField("Event", "PORT_" + type.toString());
			jGen.writeStringField("Timestamp",ts.toString());
			jGen.writeStringField("Type","OFSwitch_Stats");
			jGen.writeStringField("Description","PORT_" + type.toString() +" - On Switch: "+ dpid +" - Port Number: " + Port.getPortNo());
			jGen.writeFieldName("Switch");
			jGen.writeStartObject();
				jGen.writeStringField("DatapathId", dpid.toString());
				//Flow
				jGen.writeFieldName("Flow");
				jGen.writeStartArray();
				for(OFStatsReply fr : FlowReply) {
					OFFlowStatsReply fsr = (OFFlowStatsReply) fr;
					for(OFFlowStatsEntry fse : fsr.getEntries())
					{
						jGen.writeStartObject();
						jGen.writeStringField("Version",fse.getVersion().toString());
						jGen.writeNumberField("Cookie", fse.getCookie().getValue());
						jGen.writeStringField("Table_Id", fse.getTableId().toString());
						jGen.writeNumberField("Packet_count", fse.getPacketCount().getValue());
						jGen.writeNumberField("Byte_count", fse.getByteCount().getValue());
						jGen.writeNumberField("Duration", fse.getDurationSec());
						jGen.writeNumberField("Priority", fse.getPriority());
						jGen.writeEndObject();
					}
				}
				jGen.writeEndArray();
				//Port
				jGen.writeFieldName("Port");
					jGen.writeStartArray();
						for(OFStatsReply pr : PortReply) {
							OFPortStatsReply psr = (OFPortStatsReply) pr;
							for(OFPortStatsEntry pse : psr.getEntries())
							{
								jGen.writeStartObject();
								jGen.writeStringField("Port_Number", pse.getPortNo().toString());
								jGen.writeNumberField("Receive_Packets", pse.getRxPackets().getValue());
								jGen.writeNumberField("Transmit_Packets",pse.getTxPackets().getValue());
								jGen.writeNumberField("Receive_Dropped", pse.getRxDropped().getValue());
								jGen.writeNumberField("Transmit_Dropped", pse.getTxDropped().getValue());
								jGen.writeNumberField("Duration", pse.getDurationSec());
								jGen.writeEndObject();
							}
						}
					jGen.writeEndArray();
					jGen.writeEndObject();	
			jGen.writeEndObject();			
			jGen.close();
		} catch (IOException e ) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return stm.toString();
	}
	
	private Map<DatapathId, List<OFStatsReply>> getSwitchStatistics(Set<DatapathId> dpids, OFStatsType statsType) {
		HashMap<DatapathId, List<OFStatsReply>> model = new HashMap<DatapathId, List<OFStatsReply>>();

		List<GetStatisticsThread> activeThreads = new ArrayList<GetStatisticsThread>(dpids.size());
		List<GetStatisticsThread> pendingRemovalThreads = new ArrayList<GetStatisticsThread>();
		GetStatisticsThread t;
		for (DatapathId d : dpids) {
			t = new GetStatisticsThread(d, statsType);
			activeThreads.add(t);
			t.start();
		}

		/* Join all the threads after the timeout. Set a hard timeout
		 * of 12 seconds for the threads to finish. If the thread has not
		 * finished the switch has not replied yet and therefore we won't
		 * add the switch's stats to the reply.
		 */
		for (int iSleepCycles = 0; iSleepCycles < 10; iSleepCycles++) {
			for (GetStatisticsThread curThread : activeThreads) {
				if (curThread.getState() == State.TERMINATED) {
					model.put(curThread.getSwitchId(), curThread.getStatisticsReply());
					pendingRemovalThreads.add(curThread);
				}
			}

			/* remove the threads that have completed the queries to the switches */
			for (GetStatisticsThread curThread : pendingRemovalThreads) {
				activeThreads.remove(curThread);
			}

			/* clear the list so we don't try to double remove them */
			pendingRemovalThreads.clear();

			/* if we are done finish early */
			if (activeThreads.isEmpty()) {
				break;
			}

			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				logger.error("Interrupted while waiting for statistics", e);
			}
		}

		return model;
	}

	/**
	 * Get statistics from a switch.
	 * @param switchId
	 * @param statsType
	 * @return
	 */
	@SuppressWarnings("unchecked")
	protected List<OFStatsReply> getSwitchStatistics(DatapathId switchId, OFStatsType statsType) {
		IOFSwitch sw = SwitchSvc.getSwitch(switchId);
		ListenableFuture<?> future;
		List<OFStatsReply> values = null;
		Match match;
		if (sw != null) {
			OFStatsRequest<?> req = null;
			switch (statsType) {
			case FLOW:
				match = sw.getOFFactory().buildMatch().build();
				if (sw.getOFFactory().getVersion().compareTo(OFVersion.OF_11) >= 0) {
					req = sw.getOFFactory().buildFlowStatsRequest()
							.setMatch(match)
							.setOutPort(OFPort.ANY)
							.setOutGroup(OFGroup.ANY)
							.setTableId(TableId.ALL)
							.build();
				} else{
					req = sw.getOFFactory().buildFlowStatsRequest()
							.setMatch(match)
							.setOutPort(OFPort.ANY)
							.setTableId(TableId.ALL)
							.build();
				}
				break;
			case AGGREGATE:
				match = sw.getOFFactory().buildMatch().build();
				req = sw.getOFFactory().buildAggregateStatsRequest()
						.setMatch(match)
						.setOutPort(OFPort.ANY)
						.setTableId(TableId.ALL)
						.build();
				break;
			case PORT:
				req = sw.getOFFactory().buildPortStatsRequest()
				.setPortNo(OFPort.ANY)
				.build();
				break;
			case QUEUE:
				req = sw.getOFFactory().buildQueueStatsRequest()
				.setPortNo(OFPort.ANY)
				.setQueueId(UnsignedLong.MAX_VALUE.longValue())
				.build();
				break;
			case DESC:
				req = sw.getOFFactory().buildDescStatsRequest()
				.build();
				break;
			case GROUP:
				if (sw.getOFFactory().getVersion().compareTo(OFVersion.OF_10) > 0) {
					req = sw.getOFFactory().buildGroupStatsRequest()				
							.build();
				}
				break;

			case METER:
				if (sw.getOFFactory().getVersion().compareTo(OFVersion.OF_13) >= 0) {
					req = sw.getOFFactory().buildMeterStatsRequest()
							.setMeterId(OFMeterSerializerVer13.ALL_VAL)
							.build();
				}
				break;

			case GROUP_DESC:			
				if (sw.getOFFactory().getVersion().compareTo(OFVersion.OF_10) > 0) {
					req = sw.getOFFactory().buildGroupDescStatsRequest()			
							.build();
				}
				break;

			case GROUP_FEATURES:
				if (sw.getOFFactory().getVersion().compareTo(OFVersion.OF_10) > 0) {
					req = sw.getOFFactory().buildGroupFeaturesStatsRequest()
							.build();
				}
				break;

			case METER_CONFIG:
				if (sw.getOFFactory().getVersion().compareTo(OFVersion.OF_13) >= 0) {
					req = sw.getOFFactory().buildMeterConfigStatsRequest()
							.build();
				}
				break;

			case METER_FEATURES:
				if (sw.getOFFactory().getVersion().compareTo(OFVersion.OF_13) >= 0) {
					req = sw.getOFFactory().buildMeterFeaturesStatsRequest()
							.build();
				}
				break;

			case TABLE:
				if (sw.getOFFactory().getVersion().compareTo(OFVersion.OF_10) > 0) {
					req = sw.getOFFactory().buildTableStatsRequest()
							.build();
				}
				break;

			case TABLE_FEATURES:	
				if (sw.getOFFactory().getVersion().compareTo(OFVersion.OF_10) > 0) {
					req = sw.getOFFactory().buildTableFeaturesStatsRequest()
							.build();		
				}
				break;
			case PORT_DESC:
				if (sw.getOFFactory().getVersion().compareTo(OFVersion.OF_13) >= 0) {
					req = sw.getOFFactory().buildPortDescStatsRequest()
							.build();
				}
				break;
			case EXPERIMENTER:		
			default:
				logger.error("Stats Request Type {} not implemented yet", statsType.name());
				break;
			}

			try {
				if (req != null) {
					future = sw.writeStatsRequest(req); 
					values = (List<OFStatsReply>) future.get(2000,TimeUnit.MILLISECONDS);

				}
			} catch (Exception e) {
				logger.error("Failure retrieving statistics from switch {}. {}", sw, e);
			}
		}
		return values;
	}

	
}


