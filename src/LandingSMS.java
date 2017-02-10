import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import static java.lang.System.out;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class LandingSMS extends  TimerTask{

	static Properties props = new Properties();
	static Logger logger;
	static String sql;
	static Connection conn=null,conn2=null;
	static Statement st=null,st2=null;
	static ResultSet rs;
	
	static boolean testMod = true;
	static String defaultMailReceiver;
	static String defaultSMSReceiver;
	static String deafultLanguage;
	
	static String errorMailreceiver;
	
	SimpleDateFormat fullTimeSdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
	SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
	static String startTime;
	static String endTime;
	static String lastEndTime;
	
	List<Map<String,String>> VLNChanges = new ArrayList<Map<String,String>>();
	Map<String,String> countryCodeMap = new HashMap<String,String>();
	static long period_minute = 15;
	static int sendLimit = 5;
	
	public static void main(String[] args) throws Exception {
		
		//args = new String[]{"20171011121314"};
		
		if(args.length>0){
			lastEndTime = args[0];
		}
		
		loadProperties();

		initialLog4j();
		
		Timer t = new Timer();
		t.schedule(new LandingSMS(),0L, period_minute*1000*60);
		
	}
	
	//LANDING_SMS_CONTENT
	//LANDING_SMS_CUSTOMIZE
	//LANDING_SMS_ENTERPRICE_CUSTOM
	//LANDING_SMS_LOG
	//LANDING_SMS_SETTING
	//TODO
	@Override
	public void run() {
		try {
			setTime();
			
			createConnection();
			//setCountryCode
			setCountryCode();
			//Search need send
			queryVLRChangeUser();
			
			for(Map<String,String> m: VLNChanges){
				String serviceid = m.get("SERVICEID");
				
				if(serviceid == null){
					errorHandle("Could not get serviceid.");
					continue;
				}
				
				//Check if reach daily sending limit
				if(queryLogTimes(serviceid)!=-1 && sendLimit > queryLogTimes(serviceid)){
					String priceplanId = m.get("PRICEPLANID");
					String subsidiaryId = m.get("SUBSIDIARYID");
					String GPRS = m.get("GPRS");
					String newVln = m.get("NEWVLN");
					String msisdn = m.get("MSISDN");
					String vlnType = m.get("VLNTYPE");
					
					//Get data error handle
					if(priceplanId == null){
						errorHandle(serviceid+" can't get priceplanId.");
						continue;
					}
					if(subsidiaryId == null){
						errorHandle(serviceid+" can't get subsidiaryId.");
						continue;
					}
					if(GPRS == null){
						errorHandle(serviceid+" can't get GPRS.");
						continue;
					}
					if(newVln == null){
						errorHandle(serviceid+" can't get new VLN.");
						continue;
					}
					if(msisdn == null){
						errorHandle(serviceid+" can't get msisdn.");
						continue;
					}
					
					//find country
					String country = mapingCountryCode(newVln);
					
					if(country == null){
						errorHandle(serviceid+" can't get country.");
						continue;
					}
					
					String smsIds = querySMSSetting(priceplanId, subsidiaryId, country, vlnType,GPRS);
					
					//Query personal Setting
					//default:CHT, need not to send:OFF
					String language = querySMSCustomize(serviceid);
					
					if("OFF".equalsIgnoreCase(language))
						continue;

					if(language == null || "".equals(language)){
						language = deafultLanguage;
					}
					
					//Query Extra SMS
					String extraSMSIds = querySMSEnterpriceCustom(serviceid);
					
					if(extraSMSIds!=null){
						if(smsIds==null || "".equals(smsIds)){
							smsIds = extraSMSIds;
						}else{
							smsIds = smsIds+","+extraSMSIds;
						}
					}
					
					//No smsIds is need not to send
					if(smsIds == null)
						continue;
					
					for(String smsId:smsIds.split(",") ){
						if(smsId!=null && !"".equals(smsId)){
							//Send SMS
							sendSMS(serviceid,smsId, language, msisdn);
						}	
					}
				}else{
					logger.info(serviceid+" had reached sending times limit.");
				}
			}
			closeConnection();
			lastEndTime = endTime;
		} catch (Exception e){
			errorHandle(e);
		}
	}
	
	public String mapingCountryCode(String vln){
		String country= null;
		for(int i = 1 ; i <=vln.length() ; i++){
			country = countryCodeMap.get(vln.substring(0, i));
			if(country!=null)
				break;
		}
		return country;
	}
	
	public void setTime(){
		Date now = new Date();
		if(lastEndTime==null||"".equals(lastEndTime)){
			startTime = sdf.format(new Date(now.getTime()-1000*60*10)); //預設為查詢過去10分鐘 
		}else{
			startTime = lastEndTime;
		}
		
			endTime = sdf.format(new Date(now.getTime()-1000*60*1));
		
		logger.info("Proccess "+startTime+" to "+endTime+" data.");
	}
	
	public String querySMSSetting(String priceplanId,String subsidiaryId,String country,String vlnType,String GPRS) throws SQLException{
		sql = "select A.MSG_IDS "
				+ "from LANDING_SMS_SETTING A "
				+ "where A.PRICEPLAN_ID = "+priceplanId+" AND A.SUBSIDIARY_ID = "+subsidiaryId+" AND A.COUNTRY = '"+country+"' AND A.VLNTYPE="+vlnType+" AND A.GPRS = "+GPRS+" ";
		logger.debug("Execute SQL:"+sql);
		rs = st.executeQuery(sql);
		if(rs.next())
			return rs.getString("MSG_IDS");
		return null;
	}
	
	public String querySMSCustomize(String serviceid) throws SQLException{
		sql = "select A.LANGUAGES from LANDING_SMS_CUSTOMIZE A where A.SERVICEID = "+serviceid+" ";
		logger.debug("Execute SQL:"+sql);
		rs = st.executeQuery(sql);
		if(rs.next())
			return rs.getString("LANGUAGES");
		return null;
	}
	
	public String querySMSEnterpriceCustom(String serviceid) throws SQLException{
		sql = "select A.MSG_ID "
				+ "from LANDING_SMS_ENTERPRICE_CUSTOM A "
				+ "where A.SERVICEID = "+serviceid+" ";
		
		logger.debug("Execute SQL:"+sql);
		rs = st.executeQuery(sql);
		if(rs.next())
			return rs.getString("MSG_ID");
		return null;
	}
	
	
	public boolean insertSMSLog(String serviceid,String number,String content) throws Exception{
		
		String id = null;
		sql = "select MAX(ID)+1 ID from LANDING_SMS_LOG ";
		rs = st.executeQuery(sql);
		if(rs.next())
			id = rs.getString("ID");
		
		if(id==null){
			throw new Exception("Can't get log id");
		}
		sql = "insert into LANDING_SMS_LOG (ID,SERVICEID,SEND_NUMBER,CONTENT) "
				+ "values("+id+","+serviceid+",'"+number+"','"+content+"')";
		
		logger.debug("Execute SQL:"+sql);
		int result = st.executeUpdate(sql);
		return result==1?true:false;
		
	}
	
	public int queryLogTimes(String serviceid) throws SQLException{
		sql = "select count(1) CD "
				+ "from LANDING_SMS_LOG A "
				+ "where A.SERVICEID = "+serviceid+" and to_char(A.SEND_TIME,'yyyyMMdd') = to_char(sysdate,'yyyyMMdd') ";
		logger.debug("Execute SQL:"+sql);
		rs = st.executeQuery(sql);
		if(rs.next())
			return rs.getInt("CD");
		else
			errorHandle("At queryLogTimes can't get number.");
		return -1;
	}
	
	public void setCountryCode() throws SQLException{
		sql = "select A.COUNTRYCODE,A.COUNTRYINIT,A.COUNTRYNAME "
				+ "from COUNTRYINITIAL A";
		logger.debug("Execute SQL:"+sql);
		rs = st.executeQuery(sql);
		while(rs.next()){
			countryCodeMap.put(rs.getString("COUNTRYCODE"), rs.getString("COUNTRYINIT"));
		}
	}

	public void queryVLRChangeUser() throws SQLException{
		/**
		 * MSISDN: 用戶香港號
			SUBSIDIARYID: 用戶群組
			NEWVLN: 新的VLN
			PRICEPLANID: 資費方案
			VLR_NUMBER: VLR編號(判定國碼用)
			GPRS: 是否申請數據 (0: 有申請數據, 1: 沒申請數據)
			VLNTYPE: VLN類別(0: 動態, 1: 靜態)
		 */
		VLNChanges.clear();
		sql = "SELECT A.MSISDN, A.SUBSIDIARYID, A.NEWVLN, A.PRICEPLANID, A.VLR_NUMBER, NVL(B.STATUS,1) GPRS, A.VLNTYPE,A.SERVICEID "
				+ "FROM  ( SELECT DISTINCT B.SERVICECODE MSISDN, B.SUBSIDIARYID,B.SERVICEID, "
				+ "								A.MSGCONTENT NEWVLN, B.PRICEPLANID, B.VLR_NUMBER, B.VLNTYPE "
				+ "			   FROM MSSENDINGTASK A,"
				+ "                          (  SELECT B.SERVICECODE, A.VLN, A.VPLMNID, B.SUBSIDIARYID,B.SERVICEID, B.PRICEPLANID, C.VLR_NUMBER, A.VLNTYPE "
				+ "                              FROM "
				+ "                                            ( SELECT DISTINCT SERVICEID, SERVICECODE VLN, 1 VPLMNID, '1' VLNTYPE, '1' STATUS "
				+ "                                              FROM SERVICE"
				+ "                                              UNION ALL"
				+ "                                              SELECT SERVICEID, VLN, VPLMNID, VLNTYPE, STATUS "
				+ "                                              FROM VLNNUMBER) A, SERVICE B, UTCN.BASICPROFILE C "
				+ "                                              WHERE A.SERVICEID=B.SERVICEID AND B.SERVICEID=C.SERVICEID AND A.STATUS=1 )  B"
				+ " "
				+ "                WHERE TO_CHAR(A.LASTSUCCESSTIME,'YYYYMMDDHH24MISS') >= '"+startTime+"'   "
				+ "                				AND TO_CHAR(A.LASTSUCCESSTIME,'YYYYMMDDHH24MISS') < '"+endTime+"' "
				+ "                                AND A.STATUS=5 AND A.SERVICECODE=B.SERVICECODE AND A.MSGCONTENT=B.VLN"
				+ "                                AND SUBSTR(A.MSGCONTENT,1,3) <> '886')  A, "
				+ " "
				+ "			( 	SELECT B.SERVICEID, B.SERVICECODE, A.STATUS "
				+ "                FROM SERVICEPARAMETER A, SERVICE B "
				+ "                  WHERE PARAMETERID=3749 AND A.SERVICEID=B.SERVICEID ) B "
				+ ""
				+ "WHERE A.MSISDN=B.SERVICECODE(+) ";
		
			logger.debug("Execute SQL:"+sql);
			rs = st2.executeQuery(sql);
			
			while(rs.next()){
				Map<String,String> m  = new HashMap<String,String>();
				m.put("SERVICEID", rs.getString("SERVICEID"));
				m.put("MSISDN", rs.getString("MSISDN"));
				m.put("SUBSIDIARYID", rs.getString("SUBSIDIARYID"));
				m.put("NEWVLN", rs.getString("NEWVLN"));
				m.put("PRICEPLANID", rs.getString("PRICEPLANID"));
				m.put("GPRS", rs.getString("GPRS"));
				m.put("VLNTYPE", rs.getString("VLNTYPE"));
				VLNChanges.add(m);
			}		
	}
	
	static void loadProperties() throws FileNotFoundException, IOException{
		System.out.println("loadProperties...");
		String path="Log4j.properties";
		props.load(new   FileInputStream(path));
		PropertyConfigurator.configure(props);
		
		testMod = !"false".equals(props.getProperty("TestMod").trim());
		
		defaultMailReceiver = props.getProperty("DefaultMailReceiver");
		defaultSMSReceiver = props.getProperty("DefaultSMSReceiver");
		
		errorMailreceiver = props.getProperty("ErrorMailReceiver",defaultMailReceiver);
		period_minute = Long.parseLong(props.getProperty("period_minute","5"));
		deafultLanguage = props.getProperty("DeafultLanguage","");
		
		
		out.println(testMod+"\n"+defaultMailReceiver+"\n"+defaultSMSReceiver+"\n"+errorMailreceiver+"\n"+deafultLanguage+"\n"+period_minute);
		
		System.out.println("loadProperties Success!");
	}
	
	static void initialLog4j(){
		logger =Logger.getLogger(LandingSMS.class);
		logger.info("Logger Load Success!");
	}
	
	void createConnection() throws SQLException, ClassNotFoundException{
		logger.info("createConnection...");
		String url,DriverClass,UserName,PassWord;
		
		
		if(testMod){
			url=props.getProperty("Test.Oracle.URL")
					.replace("{{Host}}", props.getProperty("Test.Oracle.Host"))
					.replace("{{Port}}", props.getProperty("Test.Oracle.Port"))
					.replace("{{ServiceName}}", (props.getProperty("Test.Oracle.ServiceName")!=null?props.getProperty("Test.Oracle.ServiceName"):""))
					.replace("{{SID}}", (props.getProperty("Test.Oracle.SID")!=null?props.getProperty("Test.Oracle.SID"):""));
			
			DriverClass = props.getProperty("Test.Oracle.DriverClass");
			UserName = props.getProperty("Test.Oracle.UserName");
			PassWord = props.getProperty("Test.Oracle.PassWord");			
		}{
			url=props.getProperty("Oracle.URL")
					.replace("{{Host}}", props.getProperty("Oracle.Host"))
					.replace("{{Port}}", props.getProperty("Oracle.Port"))
					.replace("{{ServiceName}}", (props.getProperty("Oracle.ServiceName")!=null?props.getProperty("Oracle.ServiceName"):""))
					.replace("{{SID}}", (props.getProperty("Oracle.SID")!=null?props.getProperty("Oracle.SID"):""));
			
			DriverClass = props.getProperty("Oracle.DriverClass");
			UserName = props.getProperty("Oracle.UserName");
			PassWord = props.getProperty("Oracle.PassWord");
		}
		Class.forName(DriverClass);
		conn = DriverManager.getConnection(url, UserName, PassWord);
		st = conn.createStatement();
		String url2,DriverClass2,UserName2,PassWord2;
		
		if(testMod){
			url2=props.getProperty("Test.mBOSS.URL")
					.replace("{{Host}}", props.getProperty("Test.mBOSS.Host"))
					.replace("{{Port}}", props.getProperty("Test.mBOSS.Port"))
					.replace("{{ServiceName}}", (props.getProperty("Test.mBOSS.ServiceName")!=null?props.getProperty("Test.mBOSS.ServiceName"):""))
					.replace("{{SID}}", (props.getProperty("Test.mBOSS.SID")!=null?props.getProperty("Test.mBOSS.SID"):""));
			
			DriverClass2 = props.getProperty("Test.mBOSS.DriverClass");
			UserName2 = props.getProperty("Test.mBOSS.UserName");
			PassWord2 = props.getProperty("Test.mBOSS.PassWord");
			
		}{
			url2=props.getProperty("mBOSS.URL")
					.replace("{{Host}}", props.getProperty("mBOSS.Host"))
					.replace("{{Port}}", props.getProperty("mBOSS.Port"))
					.replace("{{ServiceName}}", (props.getProperty("mBOSS.ServiceName")!=null?props.getProperty("mBOSS.ServiceName"):""))
					.replace("{{SID}}", (props.getProperty("mBOSS.SID")!=null?props.getProperty("mBOSS.SID"):""));
			
			DriverClass2 = props.getProperty("mBOSS.DriverClass");
			UserName2 = props.getProperty("mBOSS.UserName");
			PassWord2 = props.getProperty("mBOSS.PassWord");
		}
		
		
		Class.forName(DriverClass2);
		conn2 = DriverManager.getConnection(url2, UserName2, PassWord2);
		st2 = conn2.createStatement();
		logger.info("createConnection Success!");
	}
	
	void closeConnection() throws SQLException{
		logger.info("closeConnection...");
		if(st!=null)
			st.close();
		if(st2!=null)
			st2.close();
		if(conn!=null)
			conn.close();
		if(conn2!=null)
			conn2.close();
		
		logger.info("closeConnection Success!");
	}
	

	
	void sendSMS(String serviceid,String smsId,String language,String sendNumber) throws Exception{
		
		
		
		if(sendNumber == null || "".equals(sendNumber)){
			errorHandle(serviceid + " without sned number.");
			return;
		}
		
		if(language == null || "".equals(language)){
			errorHandle(serviceid + " without language.");
			return;
		}
		
		String smsContent = getSMSContent(smsId,language);
		
		if(smsContent == null || "".equals(smsContent)){
			errorHandle(serviceid + "Can't find sms Content of id="+smsId+" and language="+language+".");
			return;
		}
		
		String[] paramName = null,paramValue = null;
		smsContent = processMag(smsContent,paramName,paramValue);
		logger.debug("send msg "+new String(smsContent.getBytes("ISO-8859-1"),"BIG5")+"to "+sendNumber+" .");
		String result = sendNowSMS(smsContent,sendNumber);
		
		insertSMSLog(serviceid, sendNumber, smsContent);
		
	}
	
	String getSMSContent(String smsId,String language) throws SQLException, UnsupportedEncodingException{
		String result = null;
		
		sql = "Select A.CONTENT "
				+ "from LANDING_SMS_CONTENT A "
				+ "where A.ID = "+smsId+" and A.LANGUAGE = '"+language+"' "
				+ "and A.START_DATE<=to_char(sysdate,'yyyyMMdd') "
				+ "and(A.END_DATE is null or to_char(sysdate,'yyyyMMdd') <= A.END_DATE) ";
		
		logger.info("Execute SQL:"+sql);
		rs = st.executeQuery(sql);
		while(rs.next()){
			result = rs.getString("CONTENT");
		}
		
		//return new String(result.getBytes("ISO-8859-1"),"big5");
		return result;
	}
	
	private String processMag(String msg,String[] paramName,String[] paramValue){
		
		if(paramName!=null){
			for(int i = 0;i<paramName.length;i++){
				msg = msg.replace(paramName[i], paramValue[i]);
			}
		}
		
		return msg;
	}
	
	private String sendNowSMS(String msg,String phone) throws IOException{
		StringBuffer sb=new StringBuffer ();
		if(testMod){
			logger.debug("send SMS at test mode.");
			phone=defaultSMSReceiver;
		}
		
		String PhoneNumber=phone,Text=msg,charset="big5",InfoCharCounter=null,PID=null,DCS=null;
		String param =
				"PhoneNumber=+{{PhoneNumber}}&"
				+ "Text={{Text}}&"
				+ "charset={{charset}}&"
				+ "InfoCharCounter={{InfoCharCounter}}&"
				+ "PID={{PID}}&"
				+ "DCS={{DCS}}&"
				+ "Submit=Submit";
		
		if(PhoneNumber==null)PhoneNumber="";
		if(Text==null)Text="";
		if(charset==null)charset="";
		if(InfoCharCounter==null)InfoCharCounter="";
		if(PID==null)PID="";
		if(DCS==null)DCS="";
		param=param.replace("{{PhoneNumber}}",PhoneNumber );
		param=param.replace("{{Text}}",Text.replaceAll("\\+", "%2b") );
		param=param.replace("{{charset}}",charset );
		param=param.replace("{{InfoCharCounter}}",InfoCharCounter );
		param=param.replace("{{PID}}",PID );
		param=param.replace("{{DCS}}",DCS );
		
		
		//20151022 change ip from 192.168.10.125 to 10.42.200.100
		return HttpPost("http://10.42.200.100:8800/Send%20Text%20Message.htm", param,"");
	}
	
	public String HttpPost(String url,String param,String charset) throws IOException{
		URL obj = new URL(url);
		
		if(charset!=null && !"".equals(charset))
			param=URLEncoder.encode(param, charset);
		
		
		HttpURLConnection con =  (HttpURLConnection) obj.openConnection();
 
		//add reuqest header
		/*con.setRequestMethod("POST");
		con.setRequestProperty("User-Agent", USER_AGENT);
		con.setRequestProperty("Accept-Language", "en-US,en;q=0.5");*/
 
		// Send post request
		con.setDoOutput(true);
		
		DataOutputStream wr = new DataOutputStream(con.getOutputStream());
		wr.writeBytes(param);
		wr.flush();
		wr.close();
 
		int responseCode = con.getResponseCode();
		System.out.println("\nSending 'POST' request to URL : " + url);
		System.out.println("Post parameters : " + new String(param.getBytes("ISO8859-1")));
		System.out.println("Response Code : " + responseCode);
 
		BufferedReader in = new BufferedReader(
		        new InputStreamReader(con.getInputStream()));
		String inputLine;
		StringBuffer response = new StringBuffer();
 
		while ((inputLine = in.readLine()) != null) {
			response.append(inputLine);
		}
		in.close();
 
		//print result
		return(response.toString());
	}

	void errorHandle(String content){
		errorHandle(content,null);
	}
	
	void errorHandle(Exception e){
		errorHandle(null,e);
	}
	
	void errorHandle(String content,Exception e){
		String errorMsg = null;
		if(e!=null){
			logger.error(content, e);
			StringWriter s = new StringWriter();
			e.printStackTrace(new PrintWriter(s));
			errorMsg=s.toString();
		}
		//sendErrorMail(content+"\n"+errorMsg);
	}
	
	void sendErrorMail(String content){
		sendMail(content,errorMailreceiver);
	}
	
	void sendMail(String mailContent,String mailReceiver){
		String mailSubject="sendLandingSMS Error";
		String mailSender="LandingSMS_Server";
		
		String [] cmd=new String[3];
		cmd[0]="/bin/bash";
		cmd[1]="-c";
		cmd[2]= "/bin/echo \""+mailContent+"\" | /bin/mail -s \""+mailSubject+"\" -r "+mailSender+" "+mailReceiver;

		try{
			Process p = Runtime.getRuntime().exec (cmd);
			p.waitFor();
			if(logger!=null)
				logger.info("send mail cmd:"+cmd[2]);
			System.out.println("send mail cmd:"+cmd[2]);
		}catch (Exception e){
			if(logger!=null)
				logger.info("send mail fail:"+cmd[2]);
			System.out.println("send mail fail:"+cmd[2]);
		}
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
		}
	}
	
	void sleep(long time){
		try {
			Thread.sleep(time);
		} catch (InterruptedException e) {	}
	}

	
	
	

}
