package VEA;


import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.math.RoundingMode;
import java.sql.*;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Set;

public class engine extends Thread {

    private static final String DB_HOST = "login.compute.amazonaws.com";  //127.0.0.1
    private static final String DB_PORT = "5432";
    private static final String DB_USER = "User";
    private static final String DB_PASSWD = "$Password";
    private static final String DB_NAME = "postgres";
    private static final String DB_URL = "jdbc:postgresql://" + DB_HOST + ":" + DB_PORT + "/" + DB_NAME;
    private JSONObject jsonObj;
    private Connection conn;
    private Statement st;

    // Json get the main objects
    private JSONObject operatingDetails;
    private JSONObject hardwareDetails;
    private JSONObject applicationDetails;
    //setting two decimal format
    private DecimalFormat df = new DecimalFormat("#.##");
    private ArrayList<String> acquiredData = new ArrayList<String>();
    private ArrayList<String> queryData = new ArrayList<String>();

    private String computerID = "";
    private String ProcessorValues = "";
    private String csValues = "";


    protected engine(String json) throws ParseException {
        jsonObj = (JSONObject) new JSONParser().parse(json);
    }

    @Override
    public void run() {

        System.out.println("CURRENT THREADS: "+Thread.currentThread().getId()+ "\n");

        System.out.println("\t" + "*****************************************************" + "\n");
        System.out.println("\t"  + "*********  Start Receiving Data ***********" + "\n");
        System.out.println(" \t" + "*****************************************************"+ "\n");


        operatingDetails = (JSONObject) jsonObj.get("operatingSystem");
        hardwareDetails = (JSONObject) jsonObj.get("hardware");
        applicationDetails = (JSONObject) jsonObj.get("applications");

        computerSystem();
        processor();
        computerEvent();

        System.out.println("ALL THE DETAILS HAVE BEEN UPDATED. DATABASE CONNECTION CLOSE !!!!");

    }

    /*
     * computer System:
     * this method gets data from the json which then can be added to the computer_system Database
     * it adds these data to the acquired ArrayList for later comparision with the previous data.
     *
     * */

    private void computerSystem() {


        try {

            // get computerSystem json object
            JSONObject computerSystem = (JSONObject) hardwareDetails.get("computerSystem");

            // get serial number from the json object which will be the computer_id
            computerID = String.valueOf(computerSystem.get("serialNumber"));

            // get the name of the OS
            String osName = (String) operatingDetails.get("family");

            // get the build of the of the os
            JSONObject buildNumber = (JSONObject) operatingDetails.get("versionInfo");
            String osBuild = (String) buildNumber.get("buildNumber");

            //access json memory object
            JSONObject memory = (JSONObject) hardwareDetails.get("memory");

            // get the total RAM memory
            String totalRAM = String.valueOf(memory.get("total"));
            Long totalMemory = Long.parseLong(totalRAM);
            String ramString = Long.toString(totalMemory);

            // get the gpu details
            String display = "";
            JSONArray graphicsCards = (JSONArray) hardwareDetails.get("graphicsCards");
            for (Object o : graphicsCards) {
                JSONObject t = (JSONObject) o;
                display = String.valueOf(t.get("name"));
            }

            // get processor json object
            JSONObject processorDetails = (JSONObject) hardwareDetails.get("processor");

            // get the processorIdentifier Object
            JSONObject processorIdentifier = (JSONObject) processorDetails.get("processorIdentifier");

            //Processor name
            String processorName = (String) processorIdentifier.get("name");

            //Processor i core details
            String processorSubstring = processorName.substring(processorName.lastIndexOf(")") + 1);
            String processorICore = getProcessorIdentifier(processorSubstring);

            // get number of physical processor
            Long physical_processor = (Long) processorDetails.get("physicalProcessorCount");
            String physicalProcessor = Long.toString(physical_processor); // convert it into the string

            // get number of virtual  processor
            Long virtual_processor = (Long) processorDetails.get("logicalProcessorCount");
            String virtualProcessor = Long.toString(virtual_processor);// convert it into the string

            // get diskSpace
            String diskSpaceStr = getDiskSpace();

            // cpu type
            String cpuType = processorName.substring(processorName.lastIndexOf("@") + 1);
            String cpu = cpuType.trim();

            //get manufacturer details
            JSONObject baseboard = (JSONObject) computerSystem.get("baseboard");
            String manufacturerDetails = String.valueOf(baseboard.get("manufacturer"));
            String manufacturer = manufacturerDetails.replace("\0", "");//replaces all occurrences of '\0' to '""'

            // get processorupgarde
            String processorUpgarde = "false";

            // get ramupgarde
            String ramUpgarde = "true";


            // adding acquired values from computer_system to ArrayList acquiredData

            acquiredData.add(computerID);

            acquiredData.add(osName);

            acquiredData.add(osBuild);

            acquiredData.add(ramString);

            acquiredData.add(display);

            acquiredData.add(processorICore);

            acquiredData.add(virtualProcessor);

            acquiredData.add(physicalProcessor);

            acquiredData.add(diskSpaceStr);

            acquiredData.add(cpu);

            acquiredData.add(manufacturer);

            acquiredData.add(processorUpgarde);

            acquiredData.add(ramUpgarde);

            csValues = "VALUES ('" + computerID + "','" + osName + "', '" + osBuild + "' , '" + totalMemory + "' ,'" + display + "' ,  '" + processorICore + "', '" + virtualProcessor + "','" + physicalProcessor + "','" + diskSpaceStr + "','" + cpu + "' ,'" + manufacturer + "' , '" + processorUpgarde+ "' , '" + ramUpgarde + "' )  RETURNING uid";


        }

        catch (Exception e) { e.printStackTrace(); }

    }

    /*
     * processor:
     * this method gets data from the json which then can be added to the processor Database
     * it adds these data to the acquired ArrayList for later comparision with the previous data.
     *
     * */

    private void processor() {
        try {

            // access the processor objects
            JSONObject processorDetails = (JSONObject) hardwareDetails.get("processor");

            // get the processorID of the processor
            JSONObject processorIdentifier = (JSONObject) processorDetails.get("processorIdentifier");
            String processorID = (String) processorIdentifier.get("processorID");


            //Processor name
            String processorName = (String) processorIdentifier.get("name");

            // get the processId
            Long pidInfo = (Long) operatingDetails.get("processId");
            String processID = Long.toString(pidInfo);


            // cpu usage percentage
            double cpuPercentage =(Double) processorDetails.get("Total CPU Load");
            String cpuPercentageStr = Double.toString(cpuPercentage);



            // get the free RAM memory available
            JSONObject totalMemory = (JSONObject) hardwareDetails.get("memory");
            String availableMemory = String.valueOf(totalMemory.get("available"));
            Long free = Long.parseLong(availableMemory);


            // get the total RAM memory
            String totalRAM = String.valueOf(totalMemory.get("total"));
            Long total = Long.parseLong(totalRAM);
            Long used = total - free;
            double memory_usage_free = ((double) free / (double) total) * 100;
            double memory_usage_used = ((double) used / (double) total) * 100;


            df.setRoundingMode(RoundingMode.CEILING);

            //free memory
            String memoryFreeStr = (df.format(memory_usage_free)); // converting to two decimal points
            double memoryFee = Double.parseDouble(memoryFreeStr);


            // used memory
            String memoryUsedStr = (df.format(memory_usage_used));
            double memoryUsed = Double.parseDouble(memoryUsedStr);


            // cpu bit identify
            String cpuBit = String.valueOf(operatingDetails.get("bitness"));
            String cpuType = cpuBit +" "+"bit";


            // get the memory type of the Ram
            String memoryType = "";
            JSONArray type = (JSONArray) totalMemory.get("physicalMemory");
            for (Object o : type) {
                JSONObject t = (JSONObject) o;
                memoryType = String.valueOf(t.get("memoryType"));
            }


            // adding acquired values from processor to ArrayList

            acquiredData.add(processorID);

            acquiredData.add(processorName);

            acquiredData.add(processID);

            acquiredData.add(cpuPercentageStr);

            acquiredData.add(memoryFreeStr);

            acquiredData.add(memoryUsedStr);

            acquiredData.add(cpuType);

            acquiredData.add(computerID);

            acquiredData.add(memoryType);


            ProcessorValues = "VALUES ('" + processorID + "' ,'" + processorName + "','" + processID + "' , '" + cpuPercentage + "'  , '" + memoryFee + "', '" + memoryUsed + "',  '" + cpuType + "' , '" + computerID + "', '" + memoryType + "')RETURNING uid";

        }
        catch (Exception e) { e.printStackTrace(); }
        finally {System.out.println("ACQUIRED VALUES FROM THE FORM THE COLLECTOR:  " + acquiredData + "\n"); }
    }



    /*
     * computerEvent:
     * this method gets data from the json which then can be added to the computerEvent Database
     *
     *
     *
     * */

    private void computerEvent() {

        try {

            Long thread_count = (Long) operatingDetails.get("threadCount");
            String threadCount = Long.toString(thread_count);

            //virtual memory
            JSONObject totalMemory = (JSONObject) hardwareDetails.get("memory");
            JSONObject virtual_memory = (JSONObject) totalMemory.get("virtualMemory");
            String virtualMemory = String.valueOf(virtual_memory.get("swapTotal"));

            // process count
            String processCount = String.valueOf(operatingDetails.get("processCount"));


            // get the free RAM memory available
            String availableMemory = String.valueOf(totalMemory.get("available"));



            //application details
            System.out.println("list of application running: " + applicationDetails + "\n");


            // Connecting to the database //

            try {
                Class.forName("org.postgresql.Driver");
                conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWD);


                if (conn != null) {
                    System.out.println("Connected to the database!" + "\n");
                } else {
                    System.out.println("Failed to make connection!" + "\n");
                }
                assert conn != null;
                st = conn.createStatement();

                // query the Computer_id from the computer_system table
                String com_id = "SELECT computer_id  FROM public.computer_system where computer_id = '" + computerID + "';";
                ResultSet result_computerID;
                result_computerID = st.executeQuery(com_id);
                boolean isPresent = computerIdCheck(result_computerID);

                //to check if the  id is present or not in the computer_id column

                if (!isPresent) {// if the computer_id isn't present


                    // inserting into Computer_system
                    ResultSet rs = st.executeQuery(getCsInsert() + csValues);
                    rs.next();

                    // inserting into processor
                    ResultSet rs1 = st.executeQuery(getProcessorInsert() + ProcessorValues);
                    rs1.next();

                    application(availableMemory, virtualMemory, processCount, threadCount);
                    conn.close();
                }

                //second condition if the Computer_id is present then check if the value matched
                else{//Computer_id is present

                    String sqlRequest = "SELECT * FROM public.computer_system c " +
                            "Inner Join public.processor p ON c.computer_id = p.computer_id " +
                            " where c.computer_id = '" + computerID + "'";
                    ResultSet result = st.executeQuery(sqlRequest);
                    queryDatabase(result);

                    //check if the data in the table and the data acquired from the json data are smilier or not


                    boolean matched = acquiredData.equals(queryData); // checking for similarities between two array
                    if (!matched) {
                        System.out.println("NEW DATA HAS BEEN FOUND ! DATA WILL BE INSERTED INTO THE DATABASE WITH THE CORRESPONDING COMPUTER ID "+ "\n");

                        //this deletes the last row with the assigned Computer_ID

                        //st.executeUpdate("Delete FROM public.computer_event where computer_id = '" + computer_ID + "';");
                        st.executeUpdate("Delete FROM public.processor where computer_id = '" + computerID + "';");
                        st.executeUpdate("Delete FROM public.computer_system where computer_id = '" + computerID + "';");


                        // inserting into Computer_system
                        ResultSet rs1 = st.executeQuery(getCsInsert() + csValues);
                        rs1.next();

                        // inserting into processor
                        ResultSet rs2 = st.executeQuery(getProcessorInsert() + ProcessorValues);
                        rs2.next();

                        // check if the application is empty or not
                        application(availableMemory, virtualMemory, processCount, threadCount);


                    }
                    conn.close();
                }

            } catch (SQLException e) { System.err.format("SQL State: %s\n%s", e.getSQLState(), e.getMessage()); }
        }
        catch (Exception e) { e.printStackTrace(); }
    }


    /*
     * @param availableMemory, virtualMemory, processCount, threadCount
     *
     * application:
     * this method checks if the application object is empty or not
     * if the application is empty the connection will close
     * if not it i will loop through the application objects and get the necessary values
     * data from the json will then be added to the Computer_event Database
     *
     *if the application already with the same corresponding computer id,
     * then it will update the values otherwise it will add the data of the new application to the database
     *
     * it will look for both application name and computer_id to varify it
     *
     *
     * */
    private void application(String availableMemory, String virtualMemory, String processCount, String threadCount){
        try {
            st = conn.createStatement();// create  statement

            // check if the application is empty or not
            if (applicationDetails.isEmpty()) { // if it is empty
                System.out.println("APPLICATION DETAILS IS EMPTY" + "\n");

            } else { // if its not empty
                System.out.println("APPLICATION DETAILS FOUND" + "\n");
                System.out.println("APPLICATION DETAILS WILL BE ADDED" + "\n");

                Set keys = applicationDetails.keySet();

                // loop though the application objects to get the application(key) running  and it details
                for (Object o : keys) {
                    String key = (String) o;
                    JSONObject app = (JSONObject) applicationDetails.get(key);
                    // application name
                    String application_Name = (String) app.get("Name");

                    //application memory in percentage
                    double application_mem = (Double) app.get("%MEM");
                    String memString = (df.format(application_mem)); // converting to two decimal points
                    double mem = Double.parseDouble(memString);


                    //application cpu in percentage
                    double application_cpu = (Double) app.get("%CPU");
                    String app_cpuString = (df.format(application_cpu)); // converting to two decimal points
                    double appCPUUsage = Double.parseDouble(app_cpuString);


                    // application pid
                    Long appPID = (Long) app.get("PID");
                    //String app_pid = Long.toString(application_pid);

                    boolean applicationFound = false;
                    String application_exist = "SELECT EXISTS (SELECT true FROM public.computer_event " +
                            " where application_name = '" + key + "' and computer_id ='" + computerID + "' )";
                    ResultSet resultApp = st.executeQuery(application_exist);

                    if (resultApp.next()) {
                        applicationFound = resultApp.getBoolean(1);
                    }

                    if (applicationFound) {
                        // application_name, application_memory_usage, application_cpu_usage, application_pid ,memory_available,memory_virtual,process_count,thread_count, computer_id
                        String updateApplication = "UPDATE public.computer_event " +
                                "SET application_memory_usage = '" + mem + "' ," +
                                "application_cpu_usage = '" + appCPUUsage + "'," +
                                "application_pid = '" + appPID + "'," +
                                "memory_available = '" + availableMemory + "'," +
                                "memory_virtual = '" + virtualMemory + "'," +
                                "process_count = '" + processCount + "'," +
                                "thread_count = '" + threadCount + "'" +
                                "WHERE application_name = '" + key + "' and computer_id ='" + computerID + "';";

                        st.executeUpdate(updateApplication);
                    } else {


                        ResultSet rs3 = st.executeQuery("INSERT INTO public.computer_event" +
                                "(application_name, application_memory_usage, application_cpu_usage, application_pid ,memory_available, memory_virtual,process_count,thread_count,computer_id) " +
                                "VALUES ('" + application_Name + "', '" + mem + "', '" + appCPUUsage + "', '" + appPID + "', '" + availableMemory + "' , '" + virtualMemory + "', '" + processCount + "' , '" + threadCount + "' , '" + computerID + "') RETURNING uid");
                        rs3.next();


                    }
                }
            }
        }

        catch (SQLException e) { System.err.format("SQL State: %s\n%s", e.getSQLState(), e.getMessage()); }
        catch (Exception e) { e.printStackTrace();}
    }

    /*
     * this method queries the data from the computer_system and processor database and adds them to ArrayList QueryDatabase for comparision
     *
     * */
    private void queryDatabase(ResultSet result) {

        try {

            // iterate through the java ResultSet

            while (result.next()) {

                // computer_system data

                //computer_id,os, os_build, total_memory, gpu, processor, virtual_processor, physical_processor, diskspace_size, cpu
                queryData.add(result.getString("computer_id"));

                queryData.add(result.getString("os"));

                queryData.add(result.getString("os_build"));

                queryData.add(result.getString("total_memory"));

                queryData.add(result.getString("gpu"));

                queryData.add(result.getString("processor"));

                queryData.add(result.getString("virtual_processor"));

                queryData.add(result.getString("physical_processor"));

                queryData.add(result.getString("diskspace_size"));

                queryData.add(result.getString("manufacturer"));

                //processor system data

                //processor_id, name, pid, cpu_percentage, memory_percentage_free ,memory_percentage_used, cpu_type, computer_id ,memory_type

                queryData.add(result.getString("processor_id"));

                queryData.add(result.getString("name"));

                queryData.add(result.getString("cpu_percentage"));

                queryData.add(result.getString("memory_percentage_free"));

                queryData.add(result.getString("memory_percentage_used"));

                queryData.add(result.getString("cpu_type"));

                queryData.add(result.getString("computer_id"));

                queryData.add(result.getString("memory_type"));

            }
            System.out.println("GETTING THE PREVIOUS DATA SETS FROM THE DATABASE OF THE CORRESPONDING COMPUTER ID" + queryData + "\n");
        }
        catch (SQLException e) { System.err.format("SQL State: %s\n%s", e.getSQLState(), e.getMessage()); }
    }


    /*
     * @Param result
     * @Return isPresent
     *
     * it takes resultSet as parameters to see if the computer_id is already registered
     * the boolean is set to false
     * if it find the corresponding computer_id then the boolean will be true and get the print out
     * else boolean it set to default
     *
     *
     * */
    private boolean computerIdCheck(ResultSet result) {

        boolean isPresent = false;
        try {

            if (result.next()) {
                String getComputer_id = result.getString("computer_id");
                isPresent = true;
                System.out.println("computer_ID  " + getComputer_id + "  is already registered" + "\n");

            } else {
                System.out.println("NEW COMPUTER! COMPUTER ID WILL BE ADDED TO THE DATABASE" + "\n");
            }
        }
        catch (SQLException e) { System.err.format("SQL State: %s\n%s", e.getSQLState(), e.getMessage()); }
        catch (Exception e) { e.printStackTrace(); }
        return isPresent;
    }


    /*
     * @return diskSpaceStr
     *  Disk Space:
     *  it loops though the disk space array object
     *  if there is only one disk details then
     *  it gets the model and the disk space and add them to the ArrayList
     *  diskSpaceArray for disk Space size
     *  diskModelArray for disk Space model
     *
     * if the disk model already exits with in the ArrayList, it will continue to next index
     * with out adding those details. if it find other model then it will add them to the ArrayList
     *  and add all the disk Space details to get a total diskSpace size and return it
     *
     * */

    private String getDiskSpace() {

        // gets the model name and the size of the available disk
        ArrayList<Long> diskSpaceArray = new ArrayList<Long>(); // Store long disk space number from the json object and add it to the database
        ArrayList<String> diskModelArray = new ArrayList<String>();//Store string disk model from the json object

        String disk_Model = "";
        String diskSpaceStr = "";
        Long disk_Space = 0L;
        try {
            JSONArray diskObj = (JSONArray) hardwareDetails.get("diskStores");

            for (int i = 0; i < diskObj.size(); i++) {

                if (diskObj.size() == 1) {

                    //disk model name
                    JSONObject diskDetails = (JSONObject) diskObj.get(i);
                    disk_Model = String.valueOf(diskDetails.get("model"));
                    diskModelArray.add(disk_Model);

                    //disk size
                    String diskSize = String.valueOf(diskDetails.get("size"));
                    Long spaceSize = Long.parseLong(diskSize.trim());// change string into Long
                    diskSpaceArray.add(spaceSize); //add it to ArrayList Disks Space

                }
                // checks if the model is already present in the ArrayList to avoid duplication
                else if (diskModelArray.contains(disk_Model)) {
                    continue;

                }
                // if the model name is not present in the ArrayList then it a gets the Model and Size
                else {
                    //disk model name
                    JSONObject diskDetail = (JSONObject) diskObj.get(i);
                    disk_Model = String.valueOf(diskDetail.get("model"));
                    diskModelArray.add(disk_Model);

                    //disk size
                    String diskSize = String.valueOf(diskDetail.get("size"));
                    Long spaceSize = Long.parseLong(diskSize.trim());// change string into Long
                    diskSpaceArray.add(spaceSize); //add it to ArrayList Disks Space

                }
            }
            Long sumDiskSpace = 0L;
            for (Long aLong : diskSpaceArray) {
                sumDiskSpace += aLong;// sum the disk space from the ArrayList
            }
            disk_Space = sumDiskSpace;
            diskSpaceStr = Long.toString(disk_Space); // convert to string


        } catch (NumberFormatException nfe) { System.out.println("NumberFormatException: " + nfe.getMessage()); }
        return diskSpaceStr;
    }


    /*
     * get insert statement in computer System
     *
     * */

    private String getCsInsert() {

        String str;
        str = "INSERT INTO public.computer_system(computer_id,os, os_build, total_memory, gpu, processor, virtual_processor, physical_processor, diskspace_size, cpu, manufacturer, processor_upgrade, ram_upgard)";
        return str;
    }

    /*
     * get insert statement in processor
     * */
    private String getProcessorInsert() {

        String str;
        str = "INSERT INTO public.processor (processor_id, name, pid, cpu_percentage, memory_percentage_free ,memory_percentage_used, cpu_type, computer_id ,memory_type)";
        return str;
    }




    /*
     * get insert statement in computer_event
     * */
    private String gethealthInsert() {

        String str;
        str = "INSERT INTO public.computer_health (computer_id , rating , country)";
        return str;
    }

    /*
     * @param str
     * @return str
     *
     * this method take str and gets core substring
     * for example "i7-4650U CPU @ 1.70GHz" will get i7-4650U
     * */

    private static String getProcessorIdentifier(String str) {
        if (str.indexOf('C') >= 0) {
            str = str.substring(0, str.indexOf("C"));
        } else {
            return str;
        }
        return str;
    }

    //end
}





